//! Temporary first-party HTML media. Frozen inputs keep their exact source;
//! only unowned canonical preview results may become compact diagnostics.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{artifact, artifact_binding, artifact_version, turn_llm_context};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QuerySelect, Statement,
    TransactionTrait,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

// Used by every user list, before identity pagination. A separate real binding
// adopts the artifact; an ordinary screenshot from another tool never matches.
pub(super) fn temporary_preview_predicate(a: &str) -> String {
    format!(
        r#"COALESCE(({a}.created_by_kind='tool' AND {a}.created_by_actor_id='html_preview'
AND CASE WHEN json_valid({a}.metadata_json) THEN
 json_type({a}.metadata_json,'$.htmlTool.version')='integer'
 AND json_type({a}.metadata_json,'$.htmlTool.publication')='false'
 AND json_extract(CASE WHEN json_valid({a}.metadata_json) THEN {a}.metadata_json ELSE '{{}}' END,'$.htmlTool.version')=1
 AND json_extract(CASE WHEN json_valid({a}.metadata_json) THEN {a}.metadata_json ELSE '{{}}' END,'$.htmlTool.publication')=0
 AND json_type({a}.metadata_json,'$.htmlTool.turnId')='text'
 AND json_type({a}.metadata_json,'$.htmlTool.callId')='text'
 ELSE 0 END
AND NOT EXISTS(SELECT 1 FROM artifact_binding adopted WHERE adopted.artifact_id={a}.id AND adopted.workspace_id={a}.workspace_id AND NOT (
 adopted.binding_kind='tool_output' AND adopted.direction='output' AND adopted.role IS 'tool'
 AND adopted.turn_id IS json_extract(CASE WHEN json_valid({a}.metadata_json) THEN {a}.metadata_json ELSE '{{}}' END,'$.htmlTool.turnId')
 AND adopted.tool_call_id IS json_extract(CASE WHEN json_valid({a}.metadata_json) THEN {a}.metadata_json ELSE '{{}}' END,'$.htmlTool.callId')
 AND adopted.thread_id IS {a}.primary_thread_id AND adopted.task_id IS NULL AND adopted.task_run_id IS NULL AND adopted.message_id IS NULL))),0)"#
    )
}

fn statement(sql: impl Into<String>, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// One immutable manifest page per call. Existing logical views include shared
/// backing ranges. Completed headers cannot change identity; expiry only drops
/// a hold. The short commit rechecks the header and progress before publication.
async fn index_quantum(
    store: &CrudStore,
    after: &mut String,
    imports: &mut Option<PreviewImportIndex>,
) -> Result<bool> {
    let previous_after = after.clone();
    let row = store.connection.query_one_raw(statement("SELECT h.id,h.workspace_id,h.owner_thread,h.identity_sha256,h.message_count,h.import_count,h.imports_sha256,h.ready,h.expired,COALESCE(i.next_ordinal,0) AS next_ordinal,COALESCE(i.complete,0) AS complete,COALESCE(i.unknown,0) AS unknown FROM compaction_frozen_history h LEFT JOIN html_preview_frozen_index i ON i.manifest_id=h.id WHERE h.id>?1 ORDER BY h.id LIMIT 1",vec![after.as_str().into()])).await?;
    let Some(row) = row else {
        // Every old header was visited. Header triggers cover every insertion,
        // update, key change and deletion concurrent with this keyset traversal.
        store.run_serialized_write(||async {
            store.connection.execute_unprepared("UPDATE html_preview_index_backfill SET complete=1 WHERE singleton=1 AND complete=0").await?;
            Ok(())
        }).await?;
        after.clear();
        return Ok(false);
    };
    let discovered_id: String = row.try_get("", "id")?;
    store.run_serialized_write(||async {
        store.connection.execute_raw(statement("INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired) SELECT id,workspace_id,identity_sha256,message_count,ready,expired FROM compaction_frozen_history WHERE id=?1 ON CONFLICT DO NOTHING",vec![discovered_id.clone().into()])).await?;
        Ok(())
    }).await?;
    *after = row.try_get("", "id")?;
    // Older derived markers treated every import stream as opaque. Revisit
    // these metadata-only markers; they remain unsafe until fully verified.
    if row.try_get::<i64>("", "expired")? == 0
        && row.try_get::<i64>("", "ready")? == 1
        && row.try_get::<i64>("", "unknown")? == 1
        && row.try_get::<i64>("", "import_count")? > 0
    {
        store.run_serialized_write(||async {
            store.connection.execute_raw(statement("UPDATE html_preview_frozen_index SET next_ordinal=0,complete=0,unknown=0 WHERE manifest_id=?1 AND unknown=1 AND expired=0",vec![discovered_id.clone().into()])).await?;Ok(())
        }).await?;
        *imports = None;
        *after = previous_after;
        return Ok(true);
    }
    if row.try_get::<i64>("", "expired")? == 1
        || row.try_get::<i64>("", "ready")? != 1
        || row.try_get::<i64>("", "complete")? == 1
        || row.try_get::<i64>("", "unknown")? == 1
    {
        return Ok(true);
    }
    let id: String = row.try_get("", "id")?;
    let workspace: String = row.try_get("", "workspace_id")?;
    let owner: String = row.try_get("", "owner_thread")?;
    let count: i64 = row.try_get("", "message_count")?;
    let identity: String = row.try_get("", "identity_sha256")?;
    let next: i64 = row.try_get("", "next_ordinal")?;
    if next == count && row.try_get::<i64>("", "import_count")? > 0 {
        if !index_import_quantum(store, &row, imports).await? {
            *after = previous_after;
        }
        return Ok(true);
    }
    let page = super::compaction_frozen::compaction_frozen_history_page(
        &store.connection,
        &workspace,
        &owner,
        &id,
        u64::try_from(next)?,
    )
    .await;
    let mut turns = BTreeSet::new();
    let mut results = Vec::new();
    let mut unknown = false;
    let end = match page {
        Ok(page) => {
            for (offset, reference) in page.iter().enumerate() {
                // Legacy opaque Task input may contain media. Keep it until its
                // existing owner expires it, rather than infer an absent image.
                if reference
                    .sources
                    .iter()
                    .any(|s| s.scope.starts_with("task-basis:"))
                {
                    unknown = true;
                }
                if reference.tool_name.as_deref() != Some("html_preview") {
                    continue;
                }
                let mut found = false;
                for source in reference
                    .sources
                    .iter()
                    .chain(reference.replay_source.iter())
                {
                    if let Some(turn) = source
                        .scope
                        .strip_prefix("context:")
                        .or_else(|| source.scope.strip_prefix("item:"))
                    {
                        turns.insert(turn.to_owned());
                        found = true;
                    }
                }
                if !found {
                    unknown = true;
                }
                let source = reference
                    .replay_source
                    .as_ref()
                    .or_else(|| (reference.sources.len() == 1).then(|| &reference.sources[0]));
                if let Some(source) = source.filter(|s| s.scope.starts_with("context:")) {
                    results.push((
                        next + i64::try_from(offset)?,
                        serde_json::to_string(reference)?,
                        source.clone(),
                    ));
                } else {
                    // A legacy result without exact canonical replay cannot be
                    // authenticated as a diagnostic. Preserve its media.
                    unknown = true;
                }
            }
            next + i64::try_from(page.len())?
        }
        Err(error) => {
            if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                return Err(error);
            }
            unknown = true;
            next
        }
    };
    if turns.len() > 64 {
        unknown = true;
        turns.clear();
    }
    if unknown {
        tracing::warn!("HTML preview retention preserved an unknown frozen media scope");
    }
    store.run_serialized_write(|| async {
        let tx = store.connection.begin().await?;
        let header = tx.query_one_raw(statement("SELECT 1 AS present FROM compaction_frozen_history WHERE id=?1 AND workspace_id=?2 AND owner_thread=?3 AND ready=1 AND expired=0 AND message_count=?4 AND next_ordinal=message_count AND identity_sha256=?5",vec![id.clone().into(),workspace.clone().into(),owner.clone().into(),count.into(),identity.clone().into()])).await?;
        if header.is_none() { tx.rollback().await?; return Ok(()); }
        let changed=tx.execute_raw(statement("UPDATE html_preview_frozen_index SET next_ordinal=?2,complete=?3,unknown=?4 WHERE manifest_id=?1 AND next_ordinal=?5 AND complete=0 AND unknown=0",vec![id.clone().into(),end.into(),i64::from(end==count && row.try_get::<i64>("","import_count")?==0).into(),i64::from(unknown).into(),next.into()])).await?.rows_affected();
        if changed == 1 {
            for turn in &turns {
                tx.execute_raw(statement("INSERT INTO html_preview_frozen_turn(manifest_id,turn_id) VALUES(?1,?2) ON CONFLICT DO NOTHING",vec![id.clone().into(),turn.clone().into()])).await?;
            }
            for (ordinal, reference, source) in &results {
                let turn = source.scope.strip_prefix("context:").expect("prepared context source");
                tx.execute_raw(statement("INSERT INTO html_preview_frozen_result(manifest_id,ordinal,reference_json,turn_id,source_id,source_version) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT DO NOTHING",vec![id.clone().into(),(*ordinal).into(),reference.clone().into(),turn.into(),source.id.clone().into(),source.version.clone().into()])).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }).await?;
    if !unknown && (end < count || row.try_get::<i64>("", "import_count")? > 0) {
        *after = previous_after;
    }
    Ok(true)
}

// Existing progress owns one streaming digest, never a second graph/cache.
// A restart resumes message indexing and replays import verification from zero;
// only a complete matching digest publishes a safe marker.
struct PreviewImportIndex {
    manifest: String,
    next: u64,
    digest: Sha256,
    pending: Option<(
        super::compaction::FrozenImportRecord,
        super::checkpoint_proofs::HistoricalMember,
    )>,
}
async fn index_import_quantum(
    store: &CrudStore,
    row: &sea_orm::QueryResult,
    progress: &mut Option<PreviewImportIndex>,
) -> Result<bool> {
    let id: String = row.try_get("", "id")?;
    let workspace: String = row.try_get("", "workspace_id")?;
    let owner: String = row.try_get("", "owner_thread")?;
    let count = u64::try_from(row.try_get::<i64>("", "import_count")?)?;
    let expected: String = row.try_get("", "imports_sha256")?;
    let identity: String = row.try_get("", "identity_sha256")?;
    let messages: i64 = row.try_get("", "message_count")?;
    if progress.as_ref().is_none_or(|p| p.manifest != id) {
        *progress = Some(PreviewImportIndex {
            manifest: id.clone(),
            next: 0,
            digest: Sha256::new(),
            pending: None,
        });
    }
    let state = progress.as_mut().expect("initialized import progress");
    let verified = async {
        // Reuse the checkpoint proof's bounded historical membership walker;
        // a checkpoint summary cannot authorize arbitrary original raw leaves.
        let proof = if let Some((_, member)) = state.pending.as_mut() {
            if !member.done() {
                member.step(store).await?;
                return Ok(());
            }
            ensure!(
                member.found(),
                "preview dependency import is outside checkpoint coverage"
            );
            state.pending.take().expect("completed member").0
        } else {
            let page = super::compaction_frozen_import::compaction_frozen_import_page(
                &store.connection,
                &workspace,
                &owner,
                &id,
                state.next,
            )
            .await?;
            let proof = page
                .first()
                .ok_or_else(|| anyhow::anyhow!("preview dependency import page is missing"))?
                .clone();
            let target =
                super::checkpoint_proofs::target_reference(store, &id, proof.message_ordinal)
                    .await?;
            let checkpoint = match target.sources.as_slice() {
                [s] if s.scope.starts_with("checkpoint:") => Some(s),
                _ => None,
            };
            ensure!(
                !target.inherited
                    && target.complete
                    && !target.protected_input
                    && target
                        .context_thread
                        .as_deref()
                        .unwrap_or(&target.source_thread)
                        == owner
                    && (proof.matches_render_target(&target) || checkpoint.is_some()),
                "preview dependency import target is unknown"
            );
            // Imports convey authority for an existing message, never extra
            // media. Logical message/replay references carry its dependencies.
            super::checkpoint_proofs::original_binding(store, &workspace, &proof, true).await?;
            if let Some(checkpoint) = checkpoint {
                let member = super::checkpoint_proofs::HistoricalMember::new(
                    store,
                    &workspace,
                    &target.source_thread,
                    checkpoint,
                    &proof.source_thread,
                    proof.render_source(),
                )
                .await?;
                state.pending = Some((proof, member));
                return Ok(());
            }
            proof
        };
        let bytes = serde_json::to_vec(&proof)?;
        state.digest.update((bytes.len() as u64).to_le_bytes());
        state.digest.update(bytes);
        state.next += 1;
        ensure!(
            state.next <= count,
            "preview dependency import count mismatch"
        );
        if state.next == count {
            ensure!(
                hex::encode(state.digest.clone().finalize()) == expected,
                "preview dependency import digest mismatch"
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let unknown = match verified {
        Ok(()) => false,
        Err(error) if error.downcast_ref::<sea_orm::DbErr>().is_some() => {
            // A failed graph read may have advanced its local walker. Restart
            // the existing stream verification; never seal a partial closure.
            *progress = None;
            return Err(error);
        }
        Err(error) => {
            tracing::warn!(reason=%error,
                "HTML preview retention preserved an invalid import proof or opaque media dependency"
            );
            true
        }
    };
    let done = state.next == count || unknown;
    store.run_serialized_write(||async {
        store.connection.execute_raw(statement("UPDATE html_preview_frozen_index SET complete=?2,unknown=?3 WHERE manifest_id=?1 AND complete=0 AND unknown=0 AND next_ordinal=?7 AND identity_sha256=?6 AND workspace_id=?4 AND ready=1 AND expired=0 AND EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND h.workspace_id=?4 AND h.owner_thread=?5 AND h.ready=1 AND h.expired=0 AND h.identity_sha256=?6 AND h.message_count=?7 AND h.next_ordinal=h.message_count AND h.import_count=?8 AND h.next_import=h.import_count AND h.imports_sha256=?9)",vec![id.clone().into(),i64::from(done && !unknown).into(),i64::from(unknown).into(),workspace.clone().into(),owner.clone().into(),identity.clone().into(),messages.into(),i64::try_from(count)?.into(),expected.clone().into()])).await?;Ok(())
    }).await?;
    if done {
        *progress = None;
    }
    Ok(done)
}

/// Existing ownership is authoritative. No age or terminal status alone is a
/// release proof. Other active workspace work is a conservative short hold:
/// its in-memory provider request need not yet have a durable frozen root.
async fn unowned<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    thread: &str,
    turn: &str,
    root_execution: &str,
    readers: &crate::frozen_lifetime::ReaderCounts,
) -> Result<bool> {
    let row = db.query_one_raw(statement(r#"SELECT 1 AS safe
WHERE EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE t.id=?3 AND t.thread_id=?2 AND th.workspace_id=?1 AND t.status IN ('completed','failed','interrupted'))
AND NOT EXISTS(SELECT 1 FROM turn t JOIN thread th ON th.id=t.thread_id WHERE th.workspace_id=?1 AND t.status NOT IN ('completed','failed','interrupted'))
AND NOT EXISTS(SELECT 1 FROM turn_runtime_snapshot WHERE workspace_id=?1 OR workspace_id='' OR turn_id=?3)
AND NOT EXISTS(SELECT 1 FROM turn_llm_context WHERE turn_id=?3 AND source NOT IN ('assistant_round','tool_result_v2'))
AND NOT EXISTS(SELECT 1 FROM recovery_job WHERE turn_id=?3 AND status NOT IN ('succeeded','failed','exhausted','cancelled'))
AND NOT EXISTS(SELECT 1 FROM recovery_job WHERE turn_id=?3 AND resolution_pending=1)
AND NOT EXISTS(SELECT 1 FROM recovery_terminalization_outbox WHERE turn_id=?3 AND status NOT IN ('delivered','cancelled'))
AND NOT EXISTS(SELECT 1 FROM turn_finalization WHERE turn_id=?3 AND status='prepared')
AND NOT EXISTS(SELECT 1 FROM native_terminal_effect_outbox WHERE turn_id=?3 AND status NOT IN ('succeeded','discarded','superseded'))
AND NOT EXISTS(SELECT 1 FROM turn_event_delivery WHERE turn_id=?3 AND status NOT IN ('delivered','exhausted'))
AND NOT EXISTS(SELECT 1 FROM task_run_turn r LEFT JOIN task_run tr ON tr.id=r.run_id LEFT JOIN task t ON t.id=tr.task_id WHERE r.turn_id=?3 AND (tr.id IS NULL OR t.id IS NULL OR t.status<>'completed'))
AND NOT EXISTS(SELECT 1 FROM task_run_turn r JOIN task_delivery d ON d.task_id=r.task_id AND d.run_id=r.run_id WHERE r.turn_id=?3 AND d.status NOT IN ('delivered','failed','cancelled'))
AND NOT EXISTS(SELECT 1 FROM agent_turn_response_execution r LEFT JOIN agent_execution e ON e.id=r.execution_id WHERE r.turn_id=?3 AND e.id IS NULL)
AND NOT EXISTS(SELECT 1 FROM agent_execution e WHERE (e.id=?4 OR EXISTS(SELECT 1 FROM agent_turn_response_execution r WHERE r.turn_id=?3 AND r.execution_id=e.id)) AND e.status NOT IN ('completed','succeeded','failed','cancelled','timed_out'))
AND NOT EXISTS(SELECT 1 FROM agent_execution_resource_state s WHERE (s.execution_id=?4 OR EXISTS(SELECT 1 FROM agent_turn_response_execution r WHERE r.turn_id=?3 AND r.execution_id=s.execution_id)) AND s.status NOT IN ('completed','failed','cancelled'))
AND NOT EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE (s.workspace_id=?1 OR s.workspace_id='' OR t.workspace_id=?1) AND (s.frozen_manifest_id='' OR (s.frozen_manifest_id IS NULL AND s.history_json<>'[]')) AND (t.id IS NULL OR t.status<>'completed'))
AND NOT EXISTS(SELECT 1 FROM thread_cli_runtime_binding WHERE (workspace_id=?1 OR workspace_id='') AND (frozen_manifest_id='' OR (frozen_manifest_id IS NULL AND CASE WHEN json_valid(resume_cursor_json) THEN COALESCE(json_extract(resume_cursor_json,'$.pioneerContext.contextHistoryJson'),'[]')<>'[]' ELSE 1 END)))
AND NOT EXISTS(SELECT 1 FROM turn_cli_runtime_binding WHERE (workspace_id=?1 OR workspace_id='') AND (frozen_manifest_id='' OR (frozen_manifest_id IS NULL AND CASE WHEN json_valid(input_mapping_json) THEN COALESCE(json_extract(input_mapping_json,'$.pioneerContextBasis.historyJson'),'[]')<>'[]' ELSE 1 END)))
AND EXISTS(SELECT 1 FROM html_preview_index_backfill WHERE singleton=1 AND complete=1 AND inputs_complete=1)
AND NOT EXISTS(SELECT 1 FROM html_preview_frozen_index WHERE workspace_id=?1 AND expired=0 AND (ready<>1 OR complete<>1 OR unknown<>0))
AND NOT EXISTS(SELECT 1 FROM html_preview_frozen_turn p JOIN compaction_frozen_history h ON h.id=p.manifest_id WHERE p.turn_id=?3 AND h.workspace_id=?1 AND h.expired=0 AND (
 NOT EXISTS(SELECT 1 FROM compaction_task_output o JOIN task t ON t.id=o.task_id WHERE o.manifest_id=h.id AND t.status='completed')
 OR EXISTS(SELECT 1 FROM task_run_conversation_snapshot s LEFT JOIN task t ON t.id=s.task_id WHERE s.frozen_manifest_id=h.id AND (t.id IS NULL OR t.status<>'completed'))
 OR EXISTS(SELECT 1 FROM turn_runtime_snapshot s WHERE s.frozen_manifest_id=h.id)
 OR EXISTS(SELECT 1 FROM thread_cli_runtime_binding s WHERE s.frozen_manifest_id=h.id)
 OR EXISTS(SELECT 1 FROM turn_cli_runtime_binding s WHERE s.frozen_manifest_id=h.id)
 OR NOT EXISTS(SELECT 1 FROM html_preview_frozen_result r WHERE r.manifest_id=h.id AND r.turn_id=?3)
 OR EXISTS(SELECT 1 FROM html_preview_frozen_result r LEFT JOIN compaction_source_revision v ON v.source_id=r.source_id LEFT JOIN turn_llm_context c ON c.id=r.source_id WHERE r.manifest_id=h.id AND r.turn_id=?3 AND (r.archive_wire_sha256 IS NULL OR v.present IS NOT 1 OR NOT (r.source_version='revision:'||v.revision OR (r.source_version='revision:'||(v.revision-1) AND EXISTS(SELECT 1 FROM artifact a WHERE a.created_by_kind='tool' AND a.created_by_actor_id='html_preview' AND a.workspace_id=?1 AND CASE WHEN json_valid(a.metadata_json) THEN json_extract(a.metadata_json,'$.htmlTool.retention')='released' AND json_extract(a.metadata_json,'$.htmlTool.turnId')=r.turn_id AND json_extract(a.metadata_json,'$.htmlTool.callId')=c.item_id ELSE 0 END)))))))
"#,vec![workspace.into(),thread.into(),turn.into(),root_execution.into()])).await?;
    if row.is_none() {
        return Ok(false);
    }
    // Acquisition uses the same serialized writer as retirement. Inspect only
    // currently held roots, not the unbounded set of permanent archive roots.
    let held_ids: Vec<String> = readers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .take(65)
        .cloned()
        .collect();
    if held_ids.len() > 64 {
        return Ok(false);
    }
    for manifest in held_ids {
        if db.query_one_raw(statement("SELECT 1 AS held FROM html_preview_frozen_turn p JOIN compaction_frozen_history h ON h.id=p.manifest_id WHERE p.manifest_id=?1 AND p.turn_id=?2 AND h.workspace_id=?3 AND h.expired=0 LIMIT 1",vec![manifest.into(),turn.into(),workspace.into()])).await?.is_some() {return Ok(false);}
    }
    Ok(true)
}

/// Compact only the exact canonical preview result. Pairing, name, call id,
/// diagnostics and ordered result remain; no source/image bytes are copied.
pub fn archive_result(payload: &str) -> Result<String> {
    let mut view: Value = serde_json::from_str(payload)?;
    ensure!(
        view["kind"] == "json" && view["truncated"] == false,
        "incomplete canonical preview result"
    );
    let message = &mut view["value"];
    ensure!(
        message["role"] == "tool"
            && message["name"] == "html_preview"
            && message["tool_call_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
        "preview result identity mismatch"
    );
    let mut diagnostics: Value = serde_json::from_str(
        message["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("preview result content is not JSON"))?,
    )?;
    let object = diagnostics
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("preview diagnostics are not an object"))?;
    object.remove("llm_context");
    object.insert(
        "previewRetention".into(),
        json!("released; temporary images are no longer retained"),
    );
    message["content"] = serde_json::to_string(&diagnostics)?.into();
    message.as_object_mut().unwrap().remove("content_parts");
    Ok(serde_json::to_string(&view)?)
}

#[derive(Default)]
pub struct HtmlPreviewRetentionProgress {
    after: String,
    index_after: String,
    imports: Option<PreviewImportIndex>,
    input_after: String,
    phase: usize,
    idle: [bool; 3],
}
impl HtmlPreviewRetentionProgress {
    /// Returns false at discovery EOF. Errors do not pin the cursor to poison
    /// data; the existing maintenance owner revisits every pass/restart.
    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        while self.idle[self.phase] {
            self.phase = (self.phase + 1) % 3;
        }
        let path = self.phase;
        self.phase = (self.phase + 1) % 3;
        let more = match path {
            0 => index_quantum(store, &mut self.index_after, &mut self.imports).await?,
            1 => input_adoption_quantum(store, &mut self.input_after).await?,
            _ => self.artifact_quantum(store).await?,
        };
        self.idle[path] = !more;
        if self.idle.iter().all(|idle| *idle) {
            self.idle = [false; 3];
            Ok(false)
        } else {
            Ok(true)
        }
    }
    async fn artifact_quantum(&mut self, store: &CrudStore) -> Result<bool> {
        let row=store.connection.query_one_raw(statement("SELECT id,current_version_id,length(CAST(metadata_json AS BLOB)) AS bytes FROM artifact WHERE id>?1 AND created_by_kind='tool' AND created_by_actor_id='html_preview' ORDER BY id LIMIT 1",vec![self.after.as_str().into()])).await?;
        let Some(row) = row else {
            self.after.clear();
            return Ok(false);
        };
        self.after = row.try_get("", "id")?;
        if row
            .try_get::<Option<String>>("", "current_version_id")?
            .is_none()
        {
            return Ok(true);
        }
        if row.try_get::<i64>("", "bytes")? > 1048576 {
            tracing::warn!("HTML preview retained oversized provenance metadata");
            return Ok(true);
        }
        let artifact = artifact::Entity::find_by_id(&self.after)
            .filter(sea_orm::sea_query::Expr::cust(
                "length(CAST(metadata_json AS BLOB))<=1048576",
            ))
            .one(&store.connection)
            .await?;
        let Some(artifact) = artifact else {
            return Ok(true);
        };
        if let Err(error) = release(store, artifact).await {
            // Do not log payloads, file paths or database identifiers.
            tracing::warn!("HTML preview retention quantum preserved an uncertain candidate");
            return Err(error);
        }
        Ok(true)
    }
}

async fn release(store: &CrudStore, candidate: artifact::Model) -> Result<()> {
    let metadata: Value = serde_json::from_str(&candidate.metadata_json)?;
    let origin = &metadata["htmlTool"];
    ensure!(
        origin["version"] == 1 && origin["publication"] == false,
        "unknown preview provenance"
    );
    let turn = origin["turnId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing preview turn"))?;
    let call = origin["callId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing preview call"))?;
    let thread = candidate
        .primary_thread_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing preview thread"))?;
    let root_execution = crate::canonical_agent_id('E', &format!("root-agent-turn\0{turn}"));
    if !unowned(
        &store.connection,
        &candidate.workspace_id,
        thread,
        turn,
        &root_execution,
        &store.frozen_readers,
    )
    .await?
    {
        return Ok(());
    }
    let artifacts = artifact::Entity::find().filter(artifact::Column::WorkspaceId.eq(&candidate.workspace_id))
        .filter(artifact::Column::PrimaryThreadId.eq(thread))
        .filter(sea_orm::sea_query::Expr::cust(temporary_preview_predicate("artifact")))
        .filter(sea_orm::sea_query::Expr::cust_with_values("json_extract(CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{}' END,'$.htmlTool.turnId')=? AND json_extract(CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{}' END,'$.htmlTool.callId')=?",[turn,call]))
        .limit(5).all(&store.connection).await?;
    ensure!(artifacts.len() <= 4, "ambiguous preview artifact group");
    if artifacts.is_empty() {
        return Ok(());
    }
    let mut versions = Vec::new();
    let mut bindings = Vec::new();
    let mut indices = BTreeSet::new();
    for a in &artifacts {
        let meta: Value = serde_json::from_str(&a.metadata_json)?;
        let index = meta["htmlTool"]["index"].as_str().unwrap_or("");
        ensure!(
            matches!(index, "input" | "360" | "768" | "1024") && indices.insert(index.to_owned()),
            "invalid preview index"
        );
        let v = artifact_version::Entity::find()
            .filter(artifact_version::Column::ArtifactId.eq(&a.id))
            .limit(2)
            .all(&store.connection)
            .await?;
        ensure!(
            v.len() == 1
                && a.current_version_id.as_deref() == Some(v[0].id.as_str())
                && v[0].workspace_id == a.workspace_id
                && v[0].created_by_turn_id.as_deref() == Some(turn)
                && v[0].created_by_tool_call_id.as_deref() == Some(call),
            "preview version provenance mismatch"
        );
        let b = artifact_binding::Entity::find()
            .filter(artifact_binding::Column::ArtifactId.eq(&a.id))
            .limit(2)
            .all(&store.connection)
            .await?;
        ensure!(
            b.len() == 1 && b[0].artifact_version_id.as_deref() == Some(v[0].id.as_str()),
            "preview binding provenance mismatch"
        );
        ensure!(
            b[0].workspace_id == a.workspace_id
                && b[0].thread_id.as_deref() == Some(thread)
                && b[0].turn_id.as_deref() == Some(turn)
                && b[0].tool_call_id.as_deref() == Some(call)
                && b[0].binding_kind == "tool_output"
                && b[0].direction == "output"
                && b[0].role.as_deref() == Some("tool")
                && b[0].turn_item_id.is_none(),
            "unknown preview binding owner"
        );
        let version_metadata: Value = serde_json::from_str(&v[0].metadata_json)?;
        ensure!(
            version_metadata["htmlTool"] == meta["htmlTool"],
            "preview version metadata mismatch"
        );
        versions.push(v[0].clone());
        bindings.push(b[0].clone());
    }
    // Canonical call identity is also the existing durable tool item identity.
    // No v2 row is valid for a partial/failed capture; duplicate rows are unknown.
    let context_ids=store.connection.query_all_raw(statement("SELECT id,length(CAST(payload AS BLOB)) AS bytes FROM turn_llm_context WHERE turn_id=?1 AND item_id=?2 AND source='tool_result_v2' AND tool_name='html_preview' LIMIT 2",vec![turn.into(),call.into()])).await?;
    ensure!(
        context_ids.len() <= 1,
        "duplicate canonical preview results"
    );
    let mut contexts = Vec::new();
    for row in context_ids {
        let size: i64 = row.try_get("", "bytes")?;
        ensure!(
            (0..=524288).contains(&size),
            "unknown canonical preview result size"
        );
        let id: String = row.try_get("", "id")?;
        let context = turn_llm_context::Entity::find_by_id(id)
            .filter(sea_orm::sea_query::Expr::cust(
                "length(CAST(payload AS BLOB))<=524288",
            ))
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("preview result changed during preparation"))?;
        contexts.push(context);
    }
    if contexts.is_empty() {
        tracing::warn!(
            "HTML preview has no canonical readable-copy provenance; unattributed legacy cache files are preserved"
        );
    }
    let archived = contexts
        .iter()
        .map(|c| archive_result(&c.payload))
        .collect::<Result<Vec<_>>>()?;
    let mut metadata_updates = Vec::new();
    for a in &artifacts {
        let mut m: Value = serde_json::from_str(&a.metadata_json)?;
        m["htmlTool"]["retention"] = json!("released");
        let mut copies = BTreeSet::new();
        for c in &contexts {
            let view: Value = serde_json::from_str(&c.payload)?;
            if let Some(parts) = view["value"]["content_parts"].as_array() {
                for part in parts {
                    let attachment = &part["image"];
                    if attachment["artifact"]["artifact_id"] == a.id {
                        ensure!(
                            attachment["artifact"]["artifact_version_id"].as_str()
                                == a.current_version_id.as_deref(),
                            "preview copy version mismatch"
                        );
                        if let Some(path) = attachment["source"]["path"].as_str() {
                            copies.insert(path.to_owned());
                        }
                    }
                }
            }
        }
        ensure!(
            copies.len() <= 3 && copies.iter().all(|p| p.len() <= 4096),
            "preview copy metadata bound"
        );
        m["htmlTool"]["releasedCopies"] = json!(copies);
        metadata_updates.push(serde_json::to_string(&m)?);
    }
    store.run_serialized_write(|| async {
        let tx=store.connection.begin().await?;
        if !unowned(&tx,&candidate.workspace_id,thread,turn,&root_execution,&store.frozen_readers).await? { tx.rollback().await?; return Ok(()); }
        // Recheck mutable adoption/version/identity assumptions inside commit.
        for ((a,v),b) in artifacts.iter().zip(&versions).zip(&bindings) {
            ensure!(artifact::Entity::find_by_id(&a.id).one(&tx).await?.as_ref()==Some(a),"preview artifact changed");
            ensure!(artifact_version::Entity::find().filter(artifact_version::Column::ArtifactId.eq(&a.id)).limit(2).all(&tx).await?==vec![v.clone()],"preview version changed");
            ensure!(artifact_binding::Entity::find().filter(artifact_binding::Column::ArtifactId.eq(&a.id)).limit(2).all(&tx).await?==vec![b.clone()],"preview adopted concurrently");
        }
        ensure!(turn_llm_context::Entity::find().filter(turn_llm_context::Column::TurnId.eq(turn)).filter(turn_llm_context::Column::ItemId.eq(call)).filter(turn_llm_context::Column::Source.eq("tool_result_v2")).filter(turn_llm_context::Column::ToolName.eq("html_preview")).limit(2).all(&tx).await?==contexts,"preview context publication changed");
        for (c, payload) in contexts.iter().zip(&archived) {
            ensure!(turn_llm_context::Entity::find_by_id(&c.id).one(&tx).await?.as_ref()==Some(c),"preview result changed");
            tx.execute_raw(statement("UPDATE turn_llm_context SET payload=?2 WHERE id=?1",vec![c.id.clone().into(),payload.clone().into()])).await?;
        }
        for ((a,v),metadata) in artifacts.iter().zip(&versions).zip(&metadata_updates) {
            // Attribute these exact original/projection dependencies before
            // dropping references, including legacy previews without staging metadata.
            tx.execute_raw(statement("UPDATE artifact_blob SET metadata_json=json_set(CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{}' END,'$.htmlToolTemporary',1) WHERE id=?1 OR id IN (SELECT blob_id FROM artifact_projection WHERE artifact_id=?2 AND workspace_id=?3)",vec![v.blob_id.clone().into(),a.id.clone().into(),a.workspace_id.clone().into()])).await?;
            // Keep origin diagnostics and the binding scope for authorization.
            // Only these temporary versions/projections lose their byte owners.
            tx.execute_raw(statement("DELETE FROM artifact_external_ref WHERE artifact_id=?1 AND workspace_id=?2",vec![a.id.clone().into(),a.workspace_id.clone().into()])).await?;
            tx.execute_raw(statement("DELETE FROM artifact_projection WHERE artifact_id=?1 AND workspace_id=?2",vec![a.id.clone().into(),a.workspace_id.clone().into()])).await?;
            tx.execute_raw(statement("UPDATE artifact_binding SET artifact_version_id=NULL WHERE artifact_id=?1 AND workspace_id=?2",vec![a.id.clone().into(),a.workspace_id.clone().into()])).await?;
            tx.execute_raw(statement("UPDATE artifact SET current_version_id=NULL,status='deleted',metadata_json=?3 WHERE id=?1 AND workspace_id=?2",vec![a.id.clone().into(),a.workspace_id.clone().into(),metadata.clone().into()])).await?;
            tx.execute_raw(statement("DELETE FROM artifact_version WHERE id=?1 AND artifact_id=?2 AND workspace_id=?3",vec![v.id.clone().into(),a.id.clone().into(),a.workspace_id.clone().into()])).await?;
        }
        tx.commit().await?;
        Ok(())
    }).await
}

impl CrudStore {
    /// Rechecked under the external storage mutation guard by every production
    /// unlink path. Check all owners of the physical key, including projections.
    pub async fn artifact_storage_key_is_unreferenced(
        &self,
        workspace: &str,
        key: &str,
    ) -> Result<bool> {
        let row=self.connection.query_one_raw(statement("SELECT 1 AS safe WHERE NOT EXISTS(SELECT 1 FROM artifact_blob b WHERE b.workspace_id=?1 AND b.storage_key=?2 AND (EXISTS(SELECT 1 FROM artifact_version v WHERE v.blob_id=b.id) OR EXISTS(SELECT 1 FROM artifact_projection p WHERE p.blob_id=b.id)))",vec![workspace.into(),key.into()])).await?;
        Ok(row.is_some())
    }

    /// Bounded keyset discovery. Advance after a failed filesystem deletion too;
    /// a fresh pass/restart revisits its still-durable orphan row.
    pub async fn html_preview_gc_blob_page(
        &self,
        after: &str,
        cutoff_ms: i64,
    ) -> Result<(
        String,
        Vec<(String, super::artifact::ArtifactGcBlobCandidateRecord)>,
    )> {
        // Page the source rows first, even if every blob still has owners.
        // This bounds work independently of the number of GC candidates.
        let rows=self.connection.query_all_raw(statement("SELECT b.workspace_id,b.id,b.storage_key,b.size_bytes,b.created_at<=?2 AND CASE WHEN json_valid(b.metadata_json) THEN json_extract(b.metadata_json,'$.htmlToolTemporary')=1 ELSE 0 END AND NOT EXISTS(SELECT 1 FROM artifact_version v WHERE v.blob_id=b.id) AND NOT EXISTS(SELECT 1 FROM artifact_projection p WHERE p.blob_id=b.id) AS orphan FROM artifact_blob b WHERE b.id>?1 ORDER BY b.id LIMIT 16",vec![after.into(),chrono::DateTime::from_timestamp_millis(cutoff_ms).ok_or_else(||anyhow::anyhow!("invalid artifact GC cutoff"))?.fixed_offset().into()])).await?;
        let mut next = String::new();
        let mut candidates = Vec::new();
        for row in rows {
            next = row.try_get("", "id")?;
            if row.try_get::<i64>("", "orphan")? != 0 {
                candidates.push((
                    row.try_get("", "workspace_id")?,
                    super::artifact::ArtifactGcBlobCandidateRecord {
                        blob_id: next.clone(),
                        storage_key: row.try_get("", "storage_key")?,
                        size_bytes: row.try_get::<i64>("", "size_bytes")?.max(0) as u64,
                    },
                ));
            }
        }
        Ok((next, candidates))
    }
    pub async fn html_preview_released_copies_page(
        &self,
        after: &str,
    ) -> Result<(String, Option<(String, String)>)> {
        let row=self.connection.query_one_raw(statement("SELECT id,workspace_id,current_version_id,length(CAST(metadata_json AS BLOB)) AS bytes,substr(metadata_json,1,1048576) AS metadata_json FROM artifact WHERE id>?1 AND created_by_kind='tool' AND created_by_actor_id='html_preview' ORDER BY id LIMIT 1",vec![after.into()])).await?;
        let Some(row) = row else {
            return Ok((String::new(), None));
        };
        let id: String = row.try_get("", "id")?;
        if row
            .try_get::<Option<String>>("", "current_version_id")?
            .is_some()
        {
            return Ok((id, None));
        }
        if row.try_get::<i64>("", "bytes")? > 1048576 {
            tracing::warn!("HTML preview retained oversized copy metadata");
            return Ok((id, None));
        }
        let metadata: String = row.try_get("", "metadata_json")?;
        let parsed: Value = match serde_json::from_str(&metadata) {
            Ok(value) => value,
            Err(_) => return Ok((id, None)),
        };
        if parsed["htmlTool"]["retention"] != "released"
            || parsed["htmlTool"]["releasedCopies"]
                .as_array()
                .is_none_or(|paths| paths.is_empty())
        {
            return Ok((id, None));
        }
        Ok((id, Some((row.try_get("", "workspace_id")?, metadata))))
    }

    pub async fn finish_html_preview_copy_cleanup(
        &self,
        id: &str,
        previous: &str,
        metadata: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            self.connection.execute_raw(statement("UPDATE artifact SET metadata_json=?3 WHERE id=?1 AND current_version_id IS NULL AND metadata_json=?2",vec![id.into(),previous.into(),metadata.into()])).await?;
            Ok(())
        }).await
    }
}

/// Late durable delivery/retry must observe the same archived representation
/// after a release commit; it cannot republish deleted image dependencies.
pub(crate) async fn preview_result_was_released<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    call: &str,
) -> Result<bool> {
    Ok(db.query_one_raw(statement("SELECT 1 AS released FROM artifact WHERE created_by_kind='tool' AND created_by_actor_id='html_preview' AND current_version_id IS NULL AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.htmlTool.turnId')=?1 AND json_extract(metadata_json,'$.htmlTool.callId')=?2 AND json_extract(metadata_json,'$.htmlTool.retention')='released' ELSE 0 END LIMIT 1",vec![turn.into(),call.into()])).await?.is_some())
}

/// A supported canonical UserInput is a real adoption even when Message edits
/// do not create a UserMessage item. Prepare IDs outside the projection commit;
/// ordinary artifacts keep their existing binding behavior.
#[derive(Clone, Debug)]
pub(crate) struct PreparedPreviewInputAdoption {
    turn: String,
    artifact: String,
    version: Option<String>,
    binding: String,
    index: i64,
    at: chrono::DateTime<chrono::FixedOffset>,
}
impl PreparedPreviewInputAdoption {
    pub(crate) fn new(
        turn: &str,
        artifact: &str,
        version: Option<&str>,
        index: i64,
        at: chrono::DateTime<chrono::FixedOffset>,
    ) -> Self {
        Self {
            turn: turn.into(),
            artifact: artifact.into(),
            version: version.map(str::to_owned),
            binding: pioneer_protocol::generate_id(21),
            index,
            at,
        }
    }
}

pub(crate) async fn retain_preview_input<C: ConnectionTrait>(
    db: &C,
    input: &PreparedPreviewInputAdoption,
) -> Result<()> {
    let row=db.query_one_raw(statement("SELECT a.workspace_id,a.current_version_id,t.thread_id,th.workspace_id AS target_workspace FROM artifact a JOIN turn t ON t.id=?2 JOIN thread th ON th.id=t.thread_id WHERE a.id=?1 AND a.created_by_kind='tool' AND a.created_by_actor_id='html_preview' LIMIT 1",vec![input.artifact.clone().into(),input.turn.clone().into()])).await?;
    let Some(row) = row else {
        return Ok(());
    };
    let workspace: String = row.try_get("", "workspace_id")?;
    ensure!(
        row.try_get::<String>("", "target_workspace")? == workspace,
        "preview input crosses workspace"
    );
    let thread: String = row.try_get("", "thread_id")?;
    let version = input
        .version
        .clone()
        .or(row.try_get::<Option<String>>("", "current_version_id")?)
        .ok_or_else(|| anyhow::anyhow!("released preview input is unavailable"))?;
    let available=db.query_one_raw(statement("SELECT 1 AS present FROM artifact_version WHERE id=?1 AND artifact_id=?2 AND workspace_id=?3",vec![version.clone().into(),input.artifact.clone().into(),workspace.clone().into()])).await?;
    ensure!(
        available.is_some(),
        "released preview input version is unavailable"
    );
    // An immutable real UserInput binding is the existing adoption path. The
    // current input projection remains authoritative for Message rendering.
    db.execute_raw(statement("INSERT INTO artifact_binding(id,artifact_id,artifact_version_id,workspace_id,thread_id,turn_id,binding_kind,direction,item_index,role,created_at,metadata_json) SELECT ?1,?2,?3,?4,?5,?6,'user_input','input',?7,'user',?8,'{}' WHERE NOT EXISTS(SELECT 1 FROM artifact_binding WHERE artifact_id=?2 AND artifact_version_id=?3 AND workspace_id=?4 AND thread_id=?5 AND turn_id=?6 AND binding_kind='user_input' AND direction='input')",vec![input.binding.clone().into(),input.artifact.clone().into(),version.into(),workspace.into(),thread.into(),input.turn.clone().into(),input.index.into(),input.at.into()])).await?;
    Ok(())
}

async fn input_adoption_quantum(store: &CrudStore, after: &mut String) -> Result<bool> {
    // Enumerate source IDs first: no request/commit scans all input payloads.
    let row=store.connection.query_one_raw(statement("SELECT id,turn_id,input_type,input_index,created_at,CASE WHEN input_type='artifact' THEN length(CAST(payload AS BLOB)) ELSE 0 END AS bytes FROM turn_input WHERE id>?1 ORDER BY id LIMIT 1",vec![after.as_str().into()])).await?;
    let Some(row) = row else {
        store.run_serialized_write(||async {
            store.connection.execute_unprepared("UPDATE html_preview_index_backfill SET inputs_complete=1 WHERE singleton=1 AND inputs_complete=0").await?;
            Ok(())
        }).await?;
        after.clear();
        return Ok(false);
    };
    *after = row.try_get("", "id")?;
    if row.try_get::<String>("", "input_type")? != "artifact" {
        return Ok(true);
    }
    if row.try_get::<i64>("", "bytes")? > 8192 {
        tracing::warn!("HTML preview adoption backfill preserved oversized canonical input");
        return Ok(true);
    }
    let current = store
        .connection
        .query_one_raw(statement(
            "SELECT payload FROM turn_input WHERE id=?1 AND length(CAST(payload AS BLOB))<=8192",
            vec![after.as_str().into()],
        ))
        .await?;
    let Some(current) = current else {
        return Ok(true);
    };
    let payload: String = current.try_get("", "payload")?;
    let pioneer_protocol::UserInput::Artifact {
        artifact_id,
        version_id,
    } = serde_json::from_str(&payload)?
    else {
        tracing::warn!("HTML preview adoption backfill preserved unknown canonical input");
        return Ok(true);
    };
    let turn: String = row.try_get("", "turn_id")?;
    let adoption = PreparedPreviewInputAdoption::new(
        &turn,
        &artifact_id,
        version_id.as_deref(),
        row.try_get("", "input_index")?,
        row.try_get("", "created_at")?,
    );
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        let current=tx.query_one_raw(statement("SELECT 1 AS present FROM turn_input WHERE id=?1 AND turn_id=?2 AND input_type='artifact' AND payload=?3",vec![after.as_str().into(),turn.clone().into(),payload.clone().into()])).await?;
        if current.is_some(){retain_preview_input(&tx,&adoption).await?;}
        tx.commit().await?;Ok(())
    }).await?;
    Ok(true)
}

/// A bounded, derived verification receipt, not a second history payload.
/// Gateway verifies the original frozen rendering before preparing its hash.
#[derive(Clone)]
pub struct HtmlPreviewArchiveWitness {
    pub manifest: String,
    pub ordinal: i64,
    pub workspace: String,
    pub reference: pioneer_compaction::frozen::FrozenMessageRef,
    pub payload: String,
}
impl CrudStore {
    pub async fn html_preview_archive_witness_page(
        &self,
        after: &(String, i64),
    ) -> Result<(Option<(String, i64)>, Option<HtmlPreviewArchiveWitness>)> {
        // Page source rows, even if expired/already verified; never search the
        // whole candidate pool inside one background quantum.
        let row=self.connection.query_one_raw(statement("SELECT r.manifest_id,r.ordinal,r.reference_json,r.source_id,r.source_version,r.turn_id,r.archive_wire_sha256,h.workspace_id,h.expired FROM html_preview_frozen_result r LEFT JOIN compaction_frozen_history h ON h.id=r.manifest_id WHERE (r.manifest_id,r.ordinal)>(?1,?2) ORDER BY r.manifest_id,r.ordinal LIMIT 1",vec![after.0.clone().into(),after.1.into()])).await?;
        let Some(r) = row else {
            return Ok((None, None));
        };
        let manifest: String = r.try_get("", "manifest_id")?;
        let ordinal: i64 = r.try_get("", "ordinal")?;
        let next = Some((manifest.clone(), ordinal));
        if r.try_get::<Option<i64>>("", "expired")? != Some(0)
            || r.try_get::<Option<String>>("", "archive_wire_sha256")?
                .is_some()
        {
            return Ok((next, None));
        }
        let reference = match serde_json::from_str(&r.try_get::<String>("", "reference_json")?) {
            Ok(reference) => reference,
            Err(_) => {
                tracing::warn!("HTML preview archive preserved an invalid reference");
                return Ok((next, None));
            }
        };
        let workspace: String = r.try_get("", "workspace_id")?;
        let body=self.connection.query_one_raw(statement("SELECT c.payload FROM turn_llm_context c JOIN compaction_source_revision v ON v.source_id=c.id JOIN turn t ON t.id=c.turn_id JOIN thread th ON th.id=t.thread_id WHERE c.id=?1 AND c.turn_id=?2 AND c.source='tool_result_v2' AND c.tool_name='html_preview' AND v.present=1 AND 'revision:'||v.revision=?3 AND th.workspace_id=?4 AND length(CAST(c.payload AS BLOB))<=524288",vec![r.try_get::<String>("","source_id")?.into(),r.try_get::<String>("","turn_id")?.into(),r.try_get::<String>("","source_version")?.into(),workspace.clone().into()])).await?;
        if body.is_none() {
            tracing::warn!("HTML preview archive preserved an unavailable canonical revision");
        }
        let witness = body
            .map(|body| -> Result<HtmlPreviewArchiveWitness> {
                Ok(HtmlPreviewArchiveWitness {
                    manifest,
                    ordinal,
                    workspace,
                    reference,
                    payload: body.try_get("", "payload")?,
                })
            })
            .transpose()?;
        Ok((next, witness))
    }

    pub async fn record_html_preview_archive_witness(
        &self,
        witness: &HtmlPreviewArchiveWitness,
        archive_hash: &str,
    ) -> Result<()> {
        ensure!(
            archive_hash.len() == 64 && archive_hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid archival hash"
        );
        let reference = serde_json::to_string(&witness.reference)?;
        self.run_serialized_write(|| async {
            self.connection.execute_raw(statement("UPDATE html_preview_frozen_result SET archive_wire_sha256=?1 WHERE manifest_id=?2 AND ordinal=?3 AND reference_json=?4 AND archive_wire_sha256 IS NULL AND EXISTS(SELECT 1 FROM compaction_frozen_history h JOIN turn_llm_context c ON c.id=html_preview_frozen_result.source_id JOIN compaction_source_revision v ON v.source_id=c.id JOIN turn t ON t.id=c.turn_id JOIN thread th ON th.id=t.thread_id WHERE h.id=html_preview_frozen_result.manifest_id AND h.workspace_id=?5 AND h.expired=0 AND th.workspace_id=h.workspace_id AND t.thread_id=?6 AND c.turn_id=html_preview_frozen_result.turn_id AND c.source='tool_result_v2' AND c.tool_name='html_preview' AND c.payload=?7 AND v.present=1 AND 'revision:'||v.revision=html_preview_frozen_result.source_version)",vec![archive_hash.into(),witness.manifest.clone().into(),witness.ordinal.into(),reference.clone().into(),witness.workspace.clone().into(),witness.reference.source_thread.clone().into(),witness.payload.clone().into()])).await?;
            Ok(())
        }).await
    }

    /// Only an exact, previously authenticated frozen preview can use this
    /// projection. Retirement's single UPDATE advances the existing source
    /// trigger exactly once. Require that adjacent revision, a release marker
    /// and the verified diagnostic hash; a later edit invalidates the receipt.
    pub async fn html_preview_archived_frozen_result(
        &self,
        workspace: &str,
        reference: &pioneer_compaction::frozen::FrozenMessageRef,
    ) -> Result<Option<(String, String, pioneer_compaction::SourceRef)>> {
        if reference.tool_name.as_deref() != Some("html_preview") {
            return Ok(None);
        }
        let serialized = serde_json::to_string(reference)?;
        let Some(source) = reference
            .replay_source
            .as_ref()
            .or_else(|| reference.sources.first())
        else {
            return Ok(None);
        };
        let row=self.connection.query_one_raw(statement("SELECT c.payload,r.archive_wire_sha256,'revision:'||v.revision AS retired_version FROM html_preview_frozen_result r JOIN compaction_frozen_history h ON h.id=r.manifest_id JOIN turn_llm_context c ON c.id=r.source_id AND c.turn_id=r.turn_id JOIN compaction_source_revision v ON v.source_id=c.id JOIN turn t ON t.id=c.turn_id JOIN thread th ON th.id=t.thread_id WHERE r.source_id=?1 AND r.source_version=?2 AND r.reference_json=?3 AND h.workspace_id=?4 AND th.workspace_id=?4 AND t.thread_id=?5 AND c.source='tool_result_v2' AND c.tool_name='html_preview' AND v.present=1 AND r.source_version='revision:'||(v.revision-1) AND r.archive_wire_sha256 IS NOT NULL AND EXISTS(SELECT 1 FROM artifact a WHERE a.workspace_id=?4 AND a.created_by_kind='tool' AND a.created_by_actor_id='html_preview' AND CASE WHEN json_valid(a.metadata_json) THEN json_extract(a.metadata_json,'$.htmlTool.retention')='released' AND json_extract(a.metadata_json,'$.htmlTool.turnId')=c.turn_id AND json_extract(a.metadata_json,'$.htmlTool.callId')=c.item_id ELSE 0 END) AND length(CAST(c.payload AS BLOB))<=524288 LIMIT 1",vec![source.id.clone().into(),source.version.clone().into(),serialized.into(),workspace.into(),reference.source_thread.clone().into()])).await?;
        row.map(|r| {
            Ok((
                r.try_get("", "payload")?,
                r.try_get("", "archive_wire_sha256")?,
                pioneer_compaction::SourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: r.try_get("", "retired_version")?,
                },
            ))
        })
        .transpose()
    }
}
