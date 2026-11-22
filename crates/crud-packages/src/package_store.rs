use crate::mcp_audit_event;
use crate::mcp_server_catalog_snapshot;
use crate::skill_audit_event::SkillAuditEventRecord;
use crate::skill_audit_event::{self, skill_audit_event_record_from_model};
use crate::skill_dependency_snapshot::{self, SkillDependencySnapshotRecord};
use crate::skill_pack_installation::unix_to_datetime;
use crate::skill_pack_installation::{self, SkillPackInstallationRecord};
use crate::skill_pack_transactions::skill_installation_record_from_model;
use crate::skill_records::{
    SkillInstallationPatch, SkillInstallationRecord, SkillUploadSessionRecord,
    WorkspaceSkillPolicyRecord,
};
use crate::skill_upload_session::skill_upload_session_record_from_model;
use crate::skill_workspace_policy::workspace_skill_policy_record_from_model;
use crate::turn_mcp_binding::{self, TurnMcpBindingRecord};
use crate::turn_mcp_projection::{
    self, TurnMcpProjectionPersistenceError, TurnMcpProjectionRecord,
    TurnMcpProjectionReplaceOutcome, TurnMcpProjectionReplacement,
};
use crate::turn_skill_binding::TurnSkillBindingRecord;
use crate::turn_skill_binding::turn_skill_binding_record_from_model;
use crate::{
    McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
    PackageRecord, plugins,
};
use crate::{
    mcp_installation_transactions, mcp_server_installation, skill_installation, skill_lifecycle,
    skill_upload_session, skill_workspace_policy, turn_skill_binding,
};
use anyhow::Context;
use pioneer_crud_signals::gateway_control::{GatewayControlWake, control_work_change};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, retry_with_backoff,
};
use pioneer_protocol::SkillId;
use std::time::Duration;

use anyhow::{Result, bail};
use pioneer_entity::package_component;
pub use plugins::{
    PackageNativeWrite, PackageOwnershipWrite, PluginNativeWrite, PluginOwnershipWrite,
};
pub(crate) use plugins::{native_package_removal, validate_native_write_input};
use sea_orm::TransactionTrait;

pub struct PackageStore {
    connection: PostgresDatabase,
}

impl PackageStore {
    pub fn new(connection: PostgresDatabase) -> Self {
        Self { connection }
    }

    fn with_maintenance_access(&self) -> Self {
        Self::new(self.connection.maintenance())
    }

    async fn retry_write_on_conflict<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        pioneer_postgres_pools::retry_with_backoff(
            || operation(),
            pioneer_postgres_pools::is_retryable_database_conflict,
            pioneer_postgres_pools::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(
                pioneer_postgres_pools::DEFAULT_LOCK_RETRY_BASE_DELAY_MS,
            ),
        )
        .await
    }
}

impl PackageStore {
    pub async fn skill_plugin_owners(
        &self,
        ids: &[String],
    ) -> Result<Vec<(package_component::Model, Option<PackageRecord>)>> {
        let db = self.connection.begin_read().await?;
        let rows = plugins::skill_owners(&db, ids).await?;
        db.commit().await?;
        Ok(rows)
    }
    pub async fn get_plugin_selection(
        &self,
        turn: &str,
    ) -> Result<Option<pioneer_protocol::PluginSelectionSnapshot>> {
        plugins::selection(&self.connection, turn)
            .await?
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }
    pub async fn prepare_plugin_selection(
        &self,
        turn: &str,
        snapshot: &pioneer_protocol::PluginSelectionSnapshot,
    ) -> Result<()> {
        validate_selection(snapshot)?;
        if snapshot.phase != "prepared" {
            bail!("invalid plugin selection phase");
        }
        let value = serde_json::to_string(snapshot)?;
        self.retry_write_on_conflict(|| {
            let value = value.clone();
            async move { plugins::set_selection(&self.connection, turn, value).await }
        })
        .await
    }
    /// `resolved_skills` comes from the native resolver's committed event, not
    /// from the client or inferred from missing DB bindings.
    pub async fn ready_plugin_selection(
        &self,
        turn: &str,
        resolved_skills: &[pioneer_protocol::SkillId],
    ) -> Result<()> {
        let Some(expected) = plugins::selection(&self.connection, turn).await? else {
            return Ok(());
        };
        let mut snapshot: pioneer_protocol::PackageSelectionSnapshot =
            serde_json::from_str(&expected)?;
        validate_selection(&snapshot)?;
        let candidates = snapshot.children.clone();
        let mut prepared = Vec::with_capacity(candidates.len());
        for child in &candidates {
            let selected = if child.kind == "skill" {
                resolved_skills.iter().any(|id| id.as_str() == child.id)
            } else {
                plugins::has_mcp_binding(&self.connection, turn, &child.id).await?
            };
            if selected {
                prepared.push(child.clone());
            }
        }
        snapshot.children = prepared;
        snapshot.phase = "ready".into();
        let value = serde_json::to_string(&snapshot)?;
        // The prepared selection and its native bindings may change after the
        // read. Revalidate both under one short writer transaction; serialize
        // outside capacity and perform only bounded DB checks inside it.
        self.retry_write_on_conflict(|| {
            let value = value.clone();
            let expected = expected.clone();
            let snapshot = snapshot.clone();
            let candidates = candidates.clone();
            async move {
                let db = self.connection.begin().await?;
                if plugins::selection(&db, turn).await?.as_deref() != Some(expected.as_str()) {
                    bail!("plugin selection changed");
                }
                use sea_orm::EntityTrait;
                let turn_row = pioneer_entity::turn::Entity::find_by_id(turn.to_owned())
                    .one(&db)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("selection turn missing"))?;
                let thread = pioneer_entity::thread::Entity::find_by_id(turn_row.thread_id)
                    .one(&db)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("selection thread missing"))?;
                for selected in &snapshot.parents {
                    let parent = plugins::find(&db, &selected.id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("plugin missing"))?;
                    if parent.kind != selected.kind
                        || parent.workspace_id != thread.workspace_id
                        || !parent.enabled
                        || parent.state != "installed"
                        || parent.pending_json.is_some()
                        || parent.revision != selected.revision
                    {
                        bail!("plugin changed during preparation");
                    }
                }
                // Exclusion by the resolver never excuses structural identity
                // changes. Check all original candidates, including excluded
                // ones, before publishing the smaller execution snapshot.
                for child in &candidates {
                    let owner = plugins::owner(&db, &child.kind, &child.id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("plugin ownership missing"))?;
                    if owner.package_id != child.parent_id || owner.status != "installed" {
                        bail!("plugin child changed during preparation");
                    }
                }
                if candidates.iter().any(|c| c.kind == "mcp")
                    && crate::turn_mcp_binding::find_turn_mcp_projection(&db, turn)
                        .await?
                        .is_none()
                {
                    bail!("plugin MCP projection missing");
                }
                for child in &snapshot.children {
                    if child.kind == "skill"
                        && !plugins::has_skill_binding(&db, turn, &child.id).await?
                    {
                        bail!("plugin skill binding missing");
                    }
                    if child.kind == "mcp"
                        && !plugins::has_mcp_binding(&db, turn, &child.id).await?
                    {
                        bail!("plugin MCP binding missing");
                    }
                }
                plugins::set_selection(&db, turn, value).await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn plugin_turn_child_available(
        &self,
        turn: &str,
        kind: &str,
        id: &str,
        workspace: &str,
    ) -> Result<bool> {
        let Some(link) = plugins::owner(&self.connection, kind, id).await? else {
            // Native removal clears the child ID on the ownership link. A
            // cached handler still carrying the turn's owned ID must close,
            // rather than becoming an ownerless standalone capability.
            return Ok(self
                .get_plugin_selection(turn)
                .await?
                .is_none_or(|snapshot| !selection_has_child(&snapshot, kind, id)));
        };
        if !self.plugin_child_available(kind, id, workspace).await? {
            return Ok(false);
        }
        let Some(snapshot) = self.get_plugin_selection(turn).await? else {
            return Ok(false);
        };
        let parent = plugins::find(&self.connection, &link.package_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugin missing"))?;
        Ok(snapshot.phase == "ready"
            && snapshot.parents.iter().any(|p| {
                p.kind == parent.kind && p.id == parent.id && p.revision == parent.revision
            })
            && snapshot
                .children
                .iter()
                .any(|c| c.kind == kind && c.id == id && c.parent_id == parent.id))
    }
    pub async fn reserve_plugin_installation(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
    ) -> Result<()> {
        self.reserve_plugin_installation_with_metadata(parent, connection_id, now, None)
            .await
    }
    pub async fn reserve_plugin_installation_with_metadata(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        anyhow::ensure!(
            parent.kind == crate::PackageKind::Plugin,
            "Plugin facade requires Plugin kind"
        );
        self.reserve_package_installation_with_metadata(parent, connection_id, now, metadata)
            .await
    }
    pub async fn reserve_package_installation_with_metadata(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        parent.validate_domain()?;
        if parent.state != "installing"
            || parent.revision != 1
            || parent.pending_json.as_ref().is_none_or(|v| v.len() > 65536)
        {
            bail!("invalid plugin reservation");
        }
        let _: serde_json::Value = serde_json::from_str(parent.pending_json.as_deref().unwrap())?;
        let prepared = metadata
            .map(|metadata| crate::plugin_metadata::prepare(&parent.id, metadata))
            .transpose()?;
        let row = parent.clone();
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| {
            let row = row.clone();
            let prepared = prepared.clone();
            async move {
                let db = self.connection.begin().await?;
                let upload = crate::skill_upload_session::find_skill_upload_session(
                    &db,
                    &parent.source_upload_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("upload missing"))?;
                // Preparation read the immutable finalized payload. Recheck its
                // owner, expiry and purpose before atomically reserving the parent.
                if upload.workspace_id != parent.workspace_id
                    || upload.connection_id != connection_id as i64
                    || upload.purpose != parent.kind.as_str()
                    || upload.status != "finalized"
                    || upload.expires_at_unix <= now
                {
                    bail!("upload unavailable");
                }
                plugins::insert_parent(&db, &row, prepared).await?;
                crate::skill_upload_session::transition_skill_upload_status(
                    &db,
                    &parent.source_upload_id,
                    &["finalized"],
                    "consumed",
                    None,
                    Some(now),
                    None,
                    chrono::Utc::now().fixed_offset(),
                )
                .await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn list_plugin_installations(&self, workspace: &str) -> Result<Vec<PackageRecord>> {
        self.list_package_installations(workspace, crate::PackageKind::Plugin)
            .await
    }
    pub async fn list_package_installations(
        &self,
        workspace: &str,
        kind: crate::PackageKind,
    ) -> Result<Vec<PackageRecord>> {
        let db = self.connection.begin_read().await?;
        let rows = plugins::list(&db, workspace, kind).await?;
        db.commit().await?;
        Ok(rows
            .into_iter()
            .map(PackageRecord::with_error_presentation)
            .collect())
    }
    pub async fn settle_plugin_installation(
        &self,
        id: &str,
        revision: i64,
        state: &str,
        error: Option<String>,
    ) -> Result<()> {
        if !matches!(state, "installed" | "interrupted") {
            bail!("invalid plugin install outcome");
        }
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| {
            let error = error.clone();
            async move {
                let db = self.connection.begin().await?;
                plugins::settle(&db, id, revision, state, error).await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn record_plugin_component_failure(
        &self,
        write: &PluginOwnershipWrite,
        kind: &str,
        diagnostic: &str,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::record_failure(&db, write, kind, diagnostic).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
    /// Reused at admission and late dispatch. An unfinished parent is closed
    /// even if the process died before it could mark the operation interrupted.
    pub async fn plugin_child_available(
        &self,
        kind: &str,
        child: &str,
        workspace: &str,
    ) -> Result<bool> {
        let Some(link) = plugins::owner(&self.connection, kind, child).await? else {
            return Ok(true);
        };
        let Some(parent) = plugins::find(&self.connection, &link.package_id).await? else {
            return Ok(false);
        };
        Ok(parent.workspace_id == workspace
            && parent.enabled
            && parent.state == "installed"
            && parent.pending_json.is_none()
            && link.status == "installed")
    }
    /// Native MCP sessions may be recreated during the final reload while
    /// execution is still fenced by the retained parent pending plan. This
    /// check is only for process admission, never tool/capability selection.
    pub async fn plugin_child_runtime_available(
        &self,
        child: &str,
        workspace: &str,
    ) -> Result<bool> {
        let Some(link) = plugins::owner(&self.connection, "mcp", child).await? else {
            return Ok(true);
        };
        let Some(parent) = plugins::find(&self.connection, &link.package_id).await? else {
            return Ok(false);
        };
        Ok(parent.workspace_id == workspace
            && parent.enabled
            && parent.state == "installed"
            && link.status == "installed")
    }
    pub async fn prepare_plugin_reload(&self, id: &str, revision: i64) -> Result<()> {
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::prepare_reload(&db, id, revision).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn insert_plugin_installation(&self, parent: &PackageRecord) -> Result<()> {
        anyhow::ensure!(
            parent.kind == crate::PackageKind::Plugin,
            "Plugin facade requires Plugin kind"
        );
        self.insert_package_installation(parent).await
    }
    pub async fn insert_package_installation(&self, parent: &PackageRecord) -> Result<()> {
        parent.validate_domain()?;
        if parent.state != "installing" || parent.revision != 1 {
            bail!("new plugin must be installing at revision 1");
        }
        if let Some(pending) = &parent.pending_json {
            if pending.len() > 65536 {
                bail!("plugin pending plan too large");
            }
            let _: serde_json::Value = serde_json::from_str(pending)?;
        }
        let row = parent.clone();
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| {
            let row = row.clone();
            async move {
                let db = self.connection.begin().await?;
                plugins::insert_parent(&db, &row, None).await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    /// Scalars and ordered lists are read from one bounded physical reader snapshot.
    /// Mapping runs after release; writers cannot expose mixed metadata generations.
    pub async fn plugin_metadata_inventory(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, (i64, pioneer_protocol::PluginMetadata)>> {
        let mut result = std::collections::HashMap::new();
        for chunk in ids.chunks(64) {
            let snapshot = self.connection.begin_read().await?;
            let (parents, keywords, prompts) =
                crate::plugin_metadata::load(&snapshot, chunk).await?;
            snapshot.commit().await?;
            result.extend(crate::plugin_metadata::project(parents, keywords, prompts));
        }
        Ok(result)
    }
    pub async fn plugin_metadata(
        &self,
        parent: &PackageRecord,
    ) -> Result<Option<pioneer_protocol::PluginMetadata>> {
        Ok(self
            .plugin_metadata_inventory(std::slice::from_ref(&parent.id))
            .await?
            .remove(&parent.id)
            .filter(|(revision, _)| *revision == parent.revision)
            .map(|(_, metadata)| metadata))
    }
    pub async fn find_plugin_installation(&self, id: &str) -> Result<Option<PackageRecord>> {
        Ok(self
            .find_package_installation(id)
            .await?
            .filter(|row| row.kind == crate::PackageKind::Plugin))
    }
    pub async fn find_package_installation(&self, id: &str) -> Result<Option<PackageRecord>> {
        let db = self.connection.begin_read().await?;
        let row = plugins::find(&db, id).await?;
        db.commit().await?;
        Ok(row.map(PackageRecord::with_error_presentation))
    }
    pub async fn find_plugin_by_upload(&self, upload: &str) -> Result<Option<PackageRecord>> {
        let db = self.connection.begin_read().await?;
        let row = plugins::find_by_upload(&db, upload).await?;
        db.commit().await?;
        Ok(row.map(PackageRecord::with_error_presentation))
    }
    pub async fn plugin_components_inventory(
        &self,
        ids: &[String],
    ) -> Result<Vec<package_component::Model>> {
        plugins::components_for_parents(&self.connection, ids).await
    }
    pub async fn list_plugin_components(&self, id: &str) -> Result<Vec<package_component::Model>> {
        plugins::components(&self.connection, id).await
    }
    pub async fn find_skill_plugin_owner(
        &self,
        id: &pioneer_protocol::SkillId,
    ) -> Result<Option<package_component::Model>> {
        plugins::owner(&self.connection, "skill", id.as_str()).await
    }
    pub async fn find_mcp_plugin_owner(
        &self,
        id: &str,
    ) -> Result<Option<package_component::Model>> {
        plugins::owner(&self.connection, "mcp", id).await
    }
}

fn validate_selection(snapshot: &pioneer_protocol::PluginSelectionSnapshot) -> Result<()> {
    use std::collections::HashSet;
    if !matches!(snapshot.phase.as_str(), "prepared" | "ready") || snapshot.parents.is_empty() {
        bail!("invalid plugin selection");
    }
    let mut parents = HashSet::new();
    for parent in &snapshot.parents {
        if parent.id.len() != 21 || parent.revision <= 0 || !parents.insert(parent.id.as_str()) {
            bail!("invalid plugin parent selection");
        }
    }
    let mut children = HashSet::new();
    for child in &snapshot.children {
        if child.id.len() != 21
            || !matches!(child.kind.as_str(), "skill" | "mcp")
            || !parents.contains(child.parent_id.as_str())
            || !children.insert((child.kind.as_str(), child.id.as_str()))
        {
            bail!("invalid plugin child selection");
        }
    }
    Ok(())
}

#[cfg(test)]
mod selection_tests {
    use super::*;
    #[test]
    fn selection_validates_ownership_without_parent_or_child_quotas() {
        use pioneer_protocol::{
            PluginSelectedChild, PluginSelectedParent, PluginSelectionSnapshot,
        };
        let mut snapshot = PluginSelectionSnapshot {
            phase: "prepared".into(),
            parents: vec![PluginSelectedParent {
                kind: pioneer_protocol::PackageKind::Plugin,
                id: "P".repeat(21),
                revision: 1,
            }],
            children: vec![],
        };
        assert!(validate_selection(&snapshot).is_ok()); // Valid empty package.
        snapshot.children.push(PluginSelectedChild {
            kind: "skill".into(),
            id: "S".repeat(21),
            parent_id: "F".repeat(21),
        });
        assert!(validate_selection(&snapshot).is_err());
        snapshot.children[0].parent_id = snapshot.parents[0].id.clone();
        assert!(validate_selection(&snapshot).is_ok());
        assert!(selection_has_child(&snapshot, "skill", &"S".repeat(21)));
        assert!(!selection_has_child(&snapshot, "skill", &"T".repeat(21)));
        snapshot.children.push(snapshot.children[0].clone());
        assert!(validate_selection(&snapshot).is_err());
        snapshot.children = (0..256)
            .map(|index| PluginSelectedChild {
                kind: "skill".into(),
                id: format!("S{index:020}"),
                parent_id: snapshot.parents[0].id.clone(),
            })
            .collect();
        assert!(validate_selection(&snapshot).is_ok());
        snapshot.children.push(PluginSelectedChild {
            kind: "skill".into(),
            id: format!("S{:020}", 256),
            parent_id: snapshot.parents[0].id.clone(),
        });
        snapshot
            .parents
            .extend((1..65).map(|index| PluginSelectedParent {
                kind: pioneer_protocol::PackageKind::Plugin,
                id: format!("P{index:020}"),
                revision: 1,
            }));
        assert!(validate_selection(&snapshot).is_ok());
    }
}

fn selection_has_child(
    snapshot: &pioneer_protocol::PluginSelectionSnapshot,
    kind: &str,
    id: &str,
) -> bool {
    snapshot
        .children
        .iter()
        .any(|child| child.kind == kind && child.id == id)
}

impl PackageStore {
    pub async fn begin_plugin_mutation(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        state: &str,
        enabled: bool,
        pending: &str,
    ) -> Result<PackageRecord> {
        self.begin_plugin_mutation_with_policy(
            workspace, id, revision, state, enabled, pending, None,
        )
        .await
    }
    pub async fn begin_plugin_mutation_with_policy(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        state: &str,
        enabled: bool,
        pending: &str,
        allow_implicit_invocation: Option<bool>,
    ) -> Result<PackageRecord> {
        if !matches!(state, "updating" | "removing") || pending.len() > 65536 {
            bail!("invalid plugin mutation");
        }
        let _: serde_json::Value = serde_json::from_str(pending)?;
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            let row = plugins::begin_mutation(
                &db,
                workspace,
                id,
                revision,
                state,
                enabled,
                pending.into(),
                allow_implicit_invocation,
            )
            .await?;
            db.commit().await?;
            Ok(row)
        })
        .await
    }
    pub async fn finish_plugin_mutation(
        &self,
        id: &str,
        revision: i64,
        state: &str,
        error: Option<String>,
    ) -> Result<()> {
        if !matches!(state, "installed" | "interrupted") {
            bail!("invalid plugin outcome");
        }
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| {
            let error = error.clone();
            async move {
                let db = self.connection.begin().await?;
                plugins::finish_mutation(&db, id, revision, state, error).await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn replace_plugin_pending(
        &self,
        id: &str,
        revision: i64,
        pending: &str,
    ) -> Result<()> {
        if pending.len() > 65536 {
            bail!("plugin pending too large");
        }
        let _: serde_json::Value = serde_json::from_str(pending)?;
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::replace_pending(&db, id, revision, pending.into()).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn delete_plugin_parent(&self, id: &str, revision: i64) -> Result<()> {
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::delete_parent(&db, id, revision).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn interrupt_unfinished_plugins(&self) -> Result<()> {
        // Each committed batch removes itself from the predicate. Cancellation
        // or restart resumes remaining parents; no filesystem/installer work is
        // performed while holding the writer. Incomplete states already gate use.
        let maintenance = self.with_maintenance_access();
        loop {
            let count = maintenance
                .retry_write_on_conflict(|| async {
                    let db = maintenance.connection.begin().await?;
                    let count = plugins::interrupt_unfinished(&db).await?;
                    db.commit().await?;
                    Ok(count)
                })
                .await?;
            if count < 128 {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }
}

impl PackageStore {
    pub async fn plugin_workspace_native_threads(
        &self,
        workspace: &str,
        ids: &[String],
    ) -> Result<Vec<String>> {
        plugins::workspace_native_threads(&self.connection, workspace, ids).await
    }
}

impl PackageStore {
    /// Restore the still-owned bounded action after settle/reload uncertainty.
    /// Request cancellation outside this catch leaves the prior closed plan.
    pub async fn interrupt_plugin_mutation(
        &self,
        id: &str,
        revision: i64,
        pending: &str,
        error: &str,
    ) -> Result<()> {
        if pending.len() > 65536 {
            bail!("plugin pending too large");
        }
        let _: serde_json::Value = serde_json::from_str(pending)?;
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::interrupt_mutation(&db, id, revision, pending.into(), error.into()).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
}

impl PackageStore {
    pub async fn plugin_graph_stop_candidates(
        &self,
        workspace: &str,
        parent: &str,
    ) -> Result<Vec<(String, String)>> {
        plugins::graph_stop_candidates(&self.connection, workspace, parent).await
    }
}

impl PackageStore {
    /// Publish an existing OAuth operation's completed effect; the OAuth
    /// engine/persistence remains authoritative for credentials and cleanup.
    pub async fn complete_plugin_native_effect(
        &self,
        write: &PluginNativeWrite,
        workspace: &str,
        kind: &str,
    ) -> Result<()> {
        validate_native_write_input(Some(write))?;
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::validate_native_write(&db, Some(write), workspace, kind, &write.child_id)
                .await?;
            plugins::publish_native_write(&db, write, kind, false).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
}

impl PackageStore {
    pub async fn plugin_credential_retention(&self) -> Result<Vec<(String, String, String)>> {
        plugins::credential_retention(&self.connection).await
    }
}

impl PackageStore {
    pub async fn resume_plugin_mutation(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        expected_pending: &str,
        pending: &str,
        state: &str,
    ) -> Result<PackageRecord> {
        if pending.len() > 65536 || !matches!(state, "updating" | "removing") {
            bail!("invalid plugin repair");
        }
        let _: serde_json::Value = serde_json::from_str(pending)?;
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            let row = plugins::resume_mutation(
                &db,
                workspace,
                id,
                revision,
                expected_pending,
                pending,
                state,
            )
            .await?;
            db.commit().await?;
            Ok(row)
        })
        .await
    }
    pub async fn begin_plugin_package_update(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        pending: &str,
        upload_id: &str,
        connection_id: u64,
        now: i64,
        previous_pending: Option<&str>,
    ) -> Result<PackageRecord> {
        if pending.len() > 65536 {
            bail!("plugin plan exceeds bound");
        }
        let _: serde_json::Value = serde_json::from_str(pending)?;
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            let upload = crate::skill_upload_session::find_skill_upload_session(&db, upload_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("upload missing"))?;
            let package = plugins::find(&db, id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("package missing"))?;
            if upload.workspace_id != workspace
                || upload.connection_id != connection_id as i64
                || upload.purpose != package.kind.as_str()
                || upload.status != "finalized"
                || upload.expires_at_unix <= now
            {
                bail!("upload unavailable");
            }
            let parent = plugins::find(&db, id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
            let row = if let Some(previous) = previous_pending {
                plugins::resume_mutation(
                    &db, workspace, id, revision, previous, pending, "updating",
                )
                .await?
            } else {
                plugins::begin_mutation(
                    &db,
                    workspace,
                    id,
                    revision,
                    "updating",
                    parent.enabled,
                    pending.into(),
                    None,
                )
                .await?
            };
            if !crate::skill_upload_session::transition_skill_upload_status(
                &db,
                upload_id,
                &["finalized"],
                "consumed",
                None,
                Some(now),
                None,
                chrono::Utc::now().fixed_offset(),
            )
            .await?
            {
                bail!("upload changed");
            }
            db.commit().await?;
            Ok(row)
        })
        .await
    }
    pub async fn publish_plugin_package(
        &self,
        id: &str,
        revision: i64,
        name: &str,
        version: Option<&str>,
        fingerprint: &str,
        upload_id: Option<&str>,
    ) -> Result<()> {
        self.publish_plugin_package_with_metadata(
            id,
            revision,
            name,
            version,
            fingerprint,
            upload_id,
            None,
        )
        .await
    }
    pub async fn publish_plugin_package_with_metadata(
        &self,
        id: &str,
        revision: i64,
        name: &str,
        version: Option<&str>,
        fingerprint: &str,
        upload_id: Option<&str>,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        let prepared = metadata
            .map(|metadata| crate::plugin_metadata::prepare(id, metadata))
            .transpose()?;
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| {
            let prepared = prepared.clone();
            async move {
                let db = self.connection.begin().await?;
                plugins::publish_package(
                    &db,
                    id,
                    revision,
                    name,
                    version,
                    fingerprint,
                    upload_id,
                    prepared,
                    None,
                )
                .await?;
                db.commit().await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn forget_plugin_component(
        &self,
        id: &str,
        revision: i64,
        kind: &str,
        key: &str,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async move {
            let db = self.connection.begin().await?;
            plugins::forget_component(&db, id, revision, kind, key).await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
}

impl PackageStore {
    pub async fn publish_app_package(
        &self,
        id: &str,
        revision: i64,
        upload_id: Option<&str>,
        metadata: &crate::PreparedAppMetadata,
    ) -> Result<()> {
        let _runtime_change = self.connection.work_signal("apps.lifecycle").changing();
        self.retry_write_on_conflict(|| async {
            let db = self.connection.begin().await?;
            plugins::publish_package(
                &db,
                id,
                revision,
                &metadata.slug,
                None,
                &metadata.fingerprint,
                upload_id,
                None,
                Some(metadata),
            )
            .await?;
            db.commit().await?;
            Ok(())
        })
        .await
    }
}

impl PackageStore {
    pub async fn native_package_owner(
        &self,
        kind: &str,
        child: &str,
    ) -> Result<Option<pioneer_protocol::PackageOwner>> {
        let db = self.connection.begin_read().await?;
        let link = plugins::owner(&db, kind, child).await?;
        let owner = if let Some(link) = link {
            let parent = plugins::find(&db, &link.package_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("package owner missing"))?;
            Some(pioneer_protocol::PackageOwner {
                kind: parent.kind,
                package_id: parent.id,
                member_key: link.member_key,
            })
        } else {
            None
        };
        db.commit().await?;
        Ok(owner)
    }
}

impl PackageStore {
    pub async fn app_runtime_page(
        &self,
        after: &str,
        through: &str,
    ) -> Result<Vec<(String, Option<PackageRecord>)>> {
        let db = self.connection.begin_read().await?;
        let rows = plugins::app_runtime_page(&db, after, through, 64).await?;
        db.commit().await?;
        // Authored JSON parsing happens after releasing reader capacity. A
        // rejected row advances the raw frontier without blocking later Apps.
        Ok(rows
            .into_iter()
            .map(|(parent, app)| {
                let id = parent.id.clone();
                (id, PackageRecord::project(parent, None, app).ok())
            })
            .collect())
    }
}

impl PackageStore {
    pub async fn app_runtime_frontier(&self) -> Result<Option<String>> {
        let db = self.connection.begin_read().await?;
        let frontier = plugins::app_runtime_frontier(&db).await?;
        db.commit().await?;
        Ok(frontier)
    }
}

impl PackageStore {
    pub async fn replace_turn_skill_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnSkillBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        let turn_id = turn_id.to_owned();
        let prepared = turn_skill_binding::prepare_turn_skill_bindings(
            turn_id.as_str(),
            bindings,
            unix_to_datetime(event_timestamp_secs),
        );
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin turn skill projection transaction")?;
            turn_skill_binding::replace_prepared_turn_skill_bindings(
                &transaction,
                prepared.clone(),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit turn skill projection transaction")
        })
        .await
    }

    pub async fn list_workspace_skill_policies(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceSkillPolicyRecord>> {
        let rows =
            skill_workspace_policy::list_workspace_skill_policies(&self.connection, workspace_id)
                .await?;
        Ok(rows
            .into_iter()
            .map(workspace_skill_policy_record_from_model)
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn upsert_workspace_skill_policy(
        &self,
        record: &WorkspaceSkillPolicyRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.upsert_workspace_skill_policy_with_plugin_change(record, None, event_timestamp_secs)
            .await
    }

    pub async fn upsert_workspace_skill_policy_with_plugin_change(
        &self,
        record: &WorkspaceSkillPolicyRecord,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        plugins::validate_native_write_input(native)?;
        self.retry_write_on_conflict(|| async {
            let db = self.connection.begin().await?;
            plugins::validate_native_write(
                &db,
                native,
                &record.workspace_id,
                "skill",
                record.skill_id.as_str(),
            )
            .await?;
            let now = unix_to_datetime(event_timestamp_secs);
            skill_workspace_policy::upsert_workspace_skill_policy(&db, record, now, now).await?;
            if let Some(native) = native {
                plugins::publish_native_write(&db, native, "skill", false).await?;
            }
            db.commit().await?;
            Ok(())
        })
        .await
    }

    pub async fn install_skill_lifecycle_with_ownership(
        &self,
        installation: &SkillInstallationRecord,
        policy: &WorkspaceSkillPolicyRecord,
        audit_records: &[SkillAuditEventRecord],
        upload_id: Option<&str>,
        ownership: Option<&PluginOwnershipWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        skill_lifecycle::install(
            &self.connection,
            installation,
            policy,
            audit_records,
            upload_id,
            ownership,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn update_skill_lifecycle_with_plugin_change(
        &self,
        skill_id: &SkillId,
        patch: &SkillInstallationPatch,
        audit_records: &[SkillAuditEventRecord],
        upload_id: Option<&str>,
        ownership: Option<&PluginOwnershipWrite>,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        skill_lifecycle::update(
            &self.connection,
            skill_id,
            patch,
            audit_records,
            upload_id,
            ownership,
            native,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn delete_skill_installation_with_workspace_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<bool> {
        skill_lifecycle::delete_with_workspace_policy(&self.connection, workspace_id, skill_id)
            .await
    }

    pub async fn find_skill_installation(
        &self,
        skill_id: &SkillId,
    ) -> Result<Option<SkillInstallationRecord>> {
        skill_installation::find_skill_installation(&self.connection, skill_id)
            .await?
            .map(skill_installation_record_from_model)
            .transpose()
    }

    pub async fn uninstall_skill_installation_lifecycle(
        &self,
        expected: &SkillInstallationRecord,
        audit_records: &[SkillAuditEventRecord],
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.uninstall_skill_installation_lifecycle_with_plugin_change(
            expected,
            audit_records,
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn uninstall_skill_installation_lifecycle_with_plugin_change(
        &self,
        expected: &SkillInstallationRecord,
        audit_records: &[SkillAuditEventRecord],
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        skill_lifecycle::uninstall(
            &self.connection,
            expected,
            audit_records,
            native,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn insert_skill_upload_session(
        &self,
        record: &SkillUploadSessionRecord,
    ) -> Result<()> {
        let control_change = self
            .connection
            .work_signal("gateway.control")
            .changing()
            .with_keys(&[pioneer_crud_signals::gateway_control::GatewayControlWake::Skills.key()]);
        let control_result = async {
            self.retry_write_on_conflict(|| async {
                let created_at = unix_to_datetime(record.created_at_unix);
                let updated_at = unix_to_datetime(record.created_at_unix);
                skill_upload_session::insert_skill_upload_session(
                    &self.connection,
                    record,
                    created_at,
                    updated_at,
                )
                .await
            })
            .await
        }
        .await;
        if control_result.is_ok() {
            control_change.completed(true);
        }
        control_result
    }

    pub async fn find_skill_upload_session(
        &self,
        upload_id: &str,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        let row =
            skill_upload_session::find_skill_upload_session(&self.connection, upload_id).await?;
        Ok(row.map(skill_upload_session_record_from_model))
    }

    pub async fn upsert_mcp_server_installation(
        &self,
        record: &McpServerInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            mcp_server_installation::upsert_mcp_server_installation(
                &self.connection,
                record,
                now,
                now,
            )
            .await
        })
        .await
    }

    pub async fn upsert_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.upsert_mcp_server_installation_with_audit_and_ownership(
            record,
            audit,
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn upsert_mcp_server_installation_with_audit_and_ownership(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        ownership: Option<&PluginOwnershipWrite>,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.upsert_mcp_server_installation_with_plugin_native_change(
            record,
            audit,
            ownership,
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn upsert_mcp_server_installation_with_plugin_native_change(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        ownership: Option<&PluginOwnershipWrite>,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        plugins::validate_native_write_input(native)?;
        if native.is_some() && ownership.is_some() {
            bail!("ambiguous plugin write");
        }
        let prepared_link = ownership.map(|write| plugins::prepare_link(write, "mcp"));
        let now = unix_to_datetime(event_timestamp_secs);
        self.retry_write_on_conflict(|| {
            let prepared_link = prepared_link.clone();
            async move {
                let transaction = self.connection.begin().await?;
                let result = mcp_installation_transactions::publish(
                    &transaction,
                    record,
                    audit,
                    ownership,
                    native,
                    prepared_link,
                    now,
                )
                .await;
                match result {
                    Ok(id) => {
                        transaction.commit().await?;
                        Ok(id)
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }

    pub async fn find_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        let row = mcp_server_installation::find_mcp_server_installation(
            &self.connection,
            scope_kind,
            scope_key,
            name,
        )
        .await?;
        Ok(row.map(McpServerInstallationRecord::from))
    }

    pub async fn find_mcp_server_installation_by_id(
        &self,
        scope_kind: &str,
        scope_key: &str,
        id: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        let row = mcp_server_installation::find_mcp_server_installation_by_id(
            &self.connection,
            scope_kind,
            scope_key,
            id,
        )
        .await?;
        Ok(row.map(McpServerInstallationRecord::from))
    }
}

#[cfg(test)]
#[path = "package_ownership_tests.rs"]
mod ownership_tests;

impl PackageStore {
    pub async fn replace_turn_mcp_projection(
        &self,
        replacement: &TurnMcpProjectionReplacement,
    ) -> std::result::Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
    {
        let replacement = replacement.clone();
        retry_with_backoff(
            || {
                let replacement = replacement.clone();
                async move {
                    turn_mcp_projection::replace_turn_mcp_projection(&self.connection, &replacement)
                        .await
                }
            },
            TurnMcpProjectionPersistenceError::is_retryable_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn replace_turn_mcp_projection_with_authorization_context(
        &self,
        replacement: &TurnMcpProjectionReplacement,
        authorization_context_json: &str,
    ) -> std::result::Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
    {
        let replacement = replacement.clone();
        let authorization_context_json = authorization_context_json.to_owned();
        retry_with_backoff(
            || {
                let replacement = replacement.clone();
                let authorization_context_json = authorization_context_json.clone();
                async move {
                    turn_mcp_projection::replace_turn_mcp_projection_with_authorization_context(
                        &self.connection,
                        &replacement,
                        authorization_context_json.as_str(),
                    )
                    .await
                }
            },
            TurnMcpProjectionPersistenceError::is_retryable_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn list_turn_mcp_bindings(&self, turn_id: &str) -> Result<Vec<TurnMcpBindingRecord>> {
        let rows = turn_mcp_binding::list_turn_mcp_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(turn_mcp_binding::turn_mcp_binding_record_from_model)
            .collect())
    }

    pub async fn get_turn_mcp_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnMcpProjectionRecord>> {
        let row = turn_mcp_binding::find_turn_mcp_projection(&self.connection, turn_id).await?;
        row.map(|model| {
            Ok(TurnMcpProjectionRecord {
                turn_id: model.turn_id,
                workspace_id: model.workspace_id,
                projection_version: i32::try_from(model.projection_version)
                    .context("turn MCP projection version is outside the supported range")?,
                manifest_hash: model.manifest_hash,
                resolution_status: model.resolution_status,
                tool_count: i32::try_from(model.tool_count)
                    .context("turn MCP projection tool count is outside the supported range")?,
                created_at_unix: model.created_at.timestamp(),
            })
        })
        .transpose()
    }

    pub async fn list_recent_turn_mcp_bindings_for_server(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<TurnMcpBindingRecord>> {
        let rows = turn_mcp_binding::list_recent_turn_mcp_bindings_for_server(
            &self.connection,
            server_installation_id,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(turn_mcp_binding::turn_mcp_binding_record_from_model)
            .collect())
    }
}

impl PackageStore {
    pub async fn list_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        let rows = turn_skill_binding::list_turn_skill_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(turn_skill_binding_record_from_model)
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn find_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        let rows = turn_skill_binding::find_turn_skill_bindings(&self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .map(turn_skill_binding_record_from_model)
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn find_workspace_skill_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<Option<WorkspaceSkillPolicyRecord>> {
        skill_workspace_policy::find_workspace_skill_policy(
            &self.connection,
            workspace_id,
            skill_id,
        )
        .await?
        .map(workspace_skill_policy_record_from_model)
        .transpose()
    }

    pub async fn delete_workspace_skill_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<bool> {
        self.delete_workspace_skill_policy_with_plugin_change(workspace_id, skill_id, None)
            .await
    }

    pub async fn delete_workspace_skill_policy_with_plugin_change(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
        native: Option<&PluginNativeWrite>,
    ) -> Result<bool> {
        plugins::validate_native_write_input(native)?;
        self.retry_write_on_conflict(|| async {
            let db = self.connection.begin().await?;
            plugins::validate_native_write(&db, native, workspace_id, "skill", skill_id.as_str())
                .await?;
            let removed =
                skill_workspace_policy::delete_workspace_skill_policy(&db, workspace_id, skill_id)
                    .await?;
            if let Some(native) = native {
                plugins::publish_native_write(&db, native, "skill", false).await?;
            }
            db.commit().await?;
            Ok(removed)
        })
        .await
    }

    pub async fn insert_skill_installation(
        &self,
        record: &SkillInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            skill_installation::insert_standalone(
                &self.connection,
                record,
                unix_to_datetime(event_timestamp_secs),
            )
            .await
        })
        .await
    }

    pub async fn insert_skill_installation_with_policy(
        &self,
        installation: &SkillInstallationRecord,
        policy: &WorkspaceSkillPolicyRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        crate::skill_lifecycle::insert_with_policy(
            &self.connection,
            installation,
            policy,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn delete_skill_installation(&self, skill_id: &SkillId) -> Result<bool> {
        self.retry_write_on_conflict(|| async {
            skill_installation::delete_skill_installation(&self.connection, skill_id).await
        })
        .await
    }

    pub async fn list_skill_installations_scope_page(
        &self,
        source_kind: &str,
        scope_key: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<SkillInstallationRecord>> {
        skill_installation::list_skill_installations_scope_page(
            &self.connection,
            source_kind,
            scope_key,
            after,
            limit,
        )
        .await?
        .into_iter()
        .map(skill_installation_record_from_model)
        .collect()
    }

    pub async fn list_skill_installations(&self) -> Result<Vec<SkillInstallationRecord>> {
        let rows = skill_installation::list_skill_installations(&self.connection).await?;
        Ok(rows
            .into_iter()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn insert_skill_pack_installation(
        &self,
        record: &SkillPackInstallationRecord,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            skill_pack_installation::insert_skill_pack_installation(&self.connection, record).await
        })
        .await
    }

    pub async fn update_skill_upload_received_bytes(
        &self,
        upload_id: &str,
        received_bytes: u64,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.retry_write_on_conflict(|| async {
            let updated_at = unix_to_datetime(updated_at_unix);
            let row = skill_upload_session::update_skill_upload_received_bytes(
                &self.connection,
                upload_id,
                received_bytes,
                updated_at,
            )
            .await?;
            Ok(row.map(skill_upload_session_record_from_model))
        })
        .await
    }

    pub async fn transition_skill_upload_status(
        &self,
        upload_id: &str,
        expected_statuses: &[&str],
        status: &str,
        finalized_at_unix: Option<i64>,
        consumed_at_unix: Option<i64>,
        aborted_at_unix: Option<i64>,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        let control_wakes = vec![GatewayControlWake::Skills];
        let control_change =
            control_work_change(&self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            self.retry_write_on_conflict(|| async {
                let updated_at = unix_to_datetime(updated_at_unix);
                if !skill_upload_session::transition_skill_upload_status(
                    &self.connection,
                    upload_id,
                    expected_statuses,
                    status,
                    finalized_at_unix,
                    consumed_at_unix,
                    aborted_at_unix,
                    updated_at,
                )
                .await?
                {
                    return Ok(None);
                }
                Ok(
                    skill_upload_session::find_skill_upload_session(&self.connection, upload_id)
                        .await?
                        .map(skill_upload_session_record_from_model),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(value.is_some());
        }
        control_result
    }

    pub async fn list_expired_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        let rows =
            skill_upload_session::list_expired_skill_upload_sessions(&self.connection, now_unix)
                .await?;
        Ok(rows
            .into_iter()
            .map(skill_upload_session_record_from_model)
            .collect())
    }

    pub async fn list_stale_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        let rows =
            skill_upload_session::list_stale_skill_upload_sessions(&self.connection, now_unix)
                .await?;
        Ok(rows
            .into_iter()
            .map(skill_upload_session_record_from_model)
            .collect())
    }

    pub async fn list_mcp_server_installations(
        &self,
        scope_kind: &str,
        scope_key: &str,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        let rows = mcp_server_installation::list_mcp_server_installations(
            &self.connection,
            scope_kind,
            scope_key,
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(McpServerInstallationRecord::from)
            .collect())
    }

    pub async fn list_all_mcp_server_installations(
        &self,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        let rows =
            mcp_server_installation::list_all_mcp_server_installations(&self.connection).await?;
        Ok(rows
            .into_iter()
            .map(McpServerInstallationRecord::from)
            .collect())
    }

    pub async fn delete_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            mcp_server_installation::delete_mcp_server_installation(
                &self.connection,
                scope_kind,
                scope_key,
                name,
            )
            .await
        })
        .await
    }

    pub async fn delete_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
    ) -> Result<()> {
        self.delete_mcp_server_installation_with_plugin_change(record, audit, None)
            .await
    }

    pub async fn delete_mcp_server_installation_with_plugin_change(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        native: Option<&PluginNativeWrite>,
    ) -> Result<()> {
        plugins::validate_native_write_input(native)?;
        let package_removal = plugins::native_package_removal(native)?;
        self.retry_write_on_conflict(|| async {
            let transaction = self.connection.begin().await?;
            let result = crate::mcp_installation_transactions::remove(
                &transaction,
                record,
                audit,
                native,
                package_removal,
            )
            .await;
            match result {
                Ok(()) => transaction
                    .commit()
                    .await
                    .context("failed to commit MCP uninstall transaction"),
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        })
        .await
    }

    pub async fn upsert_mcp_server_catalog_snapshot(
        &self,
        record: &McpServerCatalogSnapshotRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(event_timestamp_secs);
            mcp_server_catalog_snapshot::upsert_mcp_server_catalog_snapshot(
                &self.connection,
                record,
                unix_to_datetime(record.generated_at_unix),
                now,
            )
            .await
        })
        .await
    }

    pub async fn find_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<Option<McpServerCatalogSnapshotRecord>> {
        let row = mcp_server_catalog_snapshot::find_mcp_server_catalog_snapshot(
            &self.connection,
            server_installation_id,
        )
        .await?;
        Ok(row.map(McpServerCatalogSnapshotRecord::from))
    }

    pub async fn delete_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<u64> {
        self.retry_write_on_conflict(|| async {
            mcp_server_catalog_snapshot::delete_mcp_server_catalog_snapshot(
                &self.connection,
                server_installation_id,
            )
            .await
        })
        .await
    }

    pub async fn insert_mcp_audit_event_record(&self, record: &McpAuditEventRecord) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            mcp_audit_event::insert_mcp_audit_event(&self.connection, record).await
        })
        .await
    }

    pub async fn list_recent_mcp_audit_event_records(
        &self,
        server_name: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        let rows =
            mcp_audit_event::list_recent_mcp_audit_events(&self.connection, server_name, limit)
                .await?;
        Ok(rows.into_iter().map(McpAuditEventRecord::from).collect())
    }

    pub async fn list_recent_mcp_audit_event_records_for_server_id(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        let rows = mcp_audit_event::list_recent_mcp_audit_events_for_server_id(
            &self.connection,
            server_installation_id,
            limit,
        )
        .await?;
        Ok(rows.into_iter().map(McpAuditEventRecord::from).collect())
    }

    pub async fn replace_turn_mcp_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnMcpBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            turn_mcp_binding::replace_turn_mcp_bindings(
                &self.connection,
                turn_id,
                bindings,
                unix_to_datetime(event_timestamp_secs),
            )
            .await
        })
        .await
    }

    pub async fn insert_skill_audit_event_records(
        &self,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            skill_audit_event::insert_skill_audit_events(&self.connection, None, records).await
        })
        .await
    }

    pub async fn append_skill_audit_event_records(
        &self,
        turn_id: &str,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            skill_audit_event::insert_skill_audit_events(&self.connection, Some(turn_id), records)
                .await
        })
        .await
    }

    pub async fn list_skill_audit_event_records(
        &self,
        skill_id: &SkillId,
        limit: u64,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        let rows =
            skill_audit_event::list_skill_audit_events(&self.connection, skill_id, limit).await?;
        Ok(rows
            .into_iter()
            .map(skill_audit_event_record_from_model)
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn insert_skill_dependency_snapshot_record(
        &self,
        record: &SkillDependencySnapshotRecord,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            skill_dependency_snapshot::insert_skill_dependency_snapshot(&self.connection, record)
                .await
        })
        .await
    }

    pub async fn list_turn_skill_dependency_snapshot_records(
        &self,
        turn_id: &str,
    ) -> Result<Vec<SkillDependencySnapshotRecord>> {
        crate::native_skill_audit_bundle::list_turn_skill_dependency_snapshot_records(
            &self.connection,
            turn_id,
        )
        .await
    }
}
