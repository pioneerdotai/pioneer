use crate::{CrudStore, repositories::plugins};
use anyhow::{Result, bail};
use pioneer_entity::{plugin_component, plugin_installation};
pub use plugins::PluginOwnershipWrite;

impl CrudStore {
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
        self.run_serialized_write(|| {
            let value = value.clone();
            async move { plugins::set_selection(&self.connection, turn, value).await }
        })
        .await
    }
    pub async fn ready_plugin_selection(&self, turn: &str) -> Result<()> {
        use sea_orm::TransactionTrait;
        let Some(mut snapshot) = self.get_plugin_selection(turn).await? else {
            return Ok(());
        };
        validate_selection(&snapshot)?;
        let expected = serde_json::to_string(&snapshot)?;
        snapshot.phase = "ready".into();
        let value = serde_json::to_string(&snapshot)?;
        // The prepared selection and its native bindings may change after the
        // read. Revalidate both under one short writer transaction; serialize
        // outside capacity and perform only bounded DB checks inside it.
        self.run_serialized_write(|| {
            let value = value.clone();
            let expected = expected.clone();
            let snapshot = snapshot.clone();
            async move {
                let db = self.connection.begin().await?;
                if plugins::selection(&db, turn).await?.as_deref() != Some(expected.as_str()) {
                    bail!("plugin selection changed");
                }
                for selected in &snapshot.parents {
                    let parent = plugins::find(&db, &selected.id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("plugin missing"))?;
                    if !parent.enabled
                        || parent.state != "installed"
                        || parent.revision != selected.revision
                    {
                        bail!("plugin changed during preparation");
                    }
                }
                for child in &snapshot.children {
                    let owner = plugins::owner(&db, &child.kind, &child.id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("plugin ownership missing"))?;
                    if owner.plugin_id != child.parent_id || owner.status != "installed" {
                        bail!("plugin child changed during preparation");
                    }
                    if child.kind == "skill"
                        && !plugins::has_skill_binding(&db, turn, &child.id).await?
                    {
                        bail!("plugin skill binding missing");
                    }
                    if child.kind == "mcp"
                        && crate::repositories::turn_mcp_binding::find_turn_mcp_projection(
                            &db, turn,
                        )
                        .await?
                        .is_none()
                    {
                        bail!("plugin MCP projection missing");
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
        let parent = plugins::find(&self.connection, &link.plugin_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugin missing"))?;
        Ok(snapshot.phase == "ready"
            && snapshot
                .parents
                .iter()
                .any(|p| p.id == parent.id && p.revision == parent.revision)
            && snapshot
                .children
                .iter()
                .any(|c| c.kind == kind && c.id == id && c.parent_id == parent.id))
    }
    pub async fn reserve_plugin_installation(
        &self,
        parent: &plugin_installation::Model,
        connection_id: u64,
        now: i64,
    ) -> Result<()> {
        use sea_orm::TransactionTrait;
        if parent.state != "installing"
            || parent.revision != 1
            || parent.pending_json.as_ref().is_none_or(|v| v.len() > 65536)
        {
            bail!("invalid plugin reservation");
        }
        let _: serde_json::Value = serde_json::from_str(parent.pending_json.as_deref().unwrap())?;
        let row: plugin_installation::ActiveModel = parent.clone().into();
        self.run_serialized_write(|| {
            let row = row.clone();
            async move {
                let db = self.connection.begin().await?;
                let upload = crate::repositories::skill_upload_session::find_skill_upload_session(
                    &db,
                    &parent.source_upload_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("upload missing"))?;
                // Preparation read the immutable finalized payload. Recheck its
                // owner, expiry and purpose before atomically reserving the parent.
                if upload.workspace_id != parent.workspace_id
                    || upload.connection_id != connection_id as i64
                    || upload.purpose != "plugin"
                    || upload.status != "finalized"
                    || upload.expires_at_unix <= now
                {
                    bail!("upload unavailable");
                }
                plugins::insert_parent(&db, row).await?;
                crate::repositories::skill_upload_session::transition_skill_upload_status(
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
    pub async fn list_plugin_installations(
        &self,
        workspace: &str,
    ) -> Result<Vec<plugin_installation::Model>> {
        plugins::list(&self.connection, workspace).await
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
        self.run_serialized_write(|| {
            let error = error.clone();
            async move { plugins::settle(&self.connection, id, revision, state, error).await }
        })
        .await
    }
    pub async fn record_plugin_component_failure(
        &self,
        write: &PluginOwnershipWrite,
        kind: &str,
        diagnostic: &str,
    ) -> Result<()> {
        self.run_serialized_write(|| async move {
            plugins::record_failure(&self.connection, write, kind, diagnostic).await
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
        let Some(parent) = plugins::find(&self.connection, &link.plugin_id).await? else {
            return Ok(false);
        };
        Ok(parent.workspace_id == workspace
            && parent.enabled
            && parent.state == "installed"
            && link.status == "installed")
    }
    pub async fn insert_plugin_installation(
        &self,
        parent: &plugin_installation::Model,
    ) -> Result<()> {
        if parent.state != "installing" || parent.revision != 1 {
            bail!("new plugin must be installing at revision 1");
        }
        if let Some(pending) = &parent.pending_json {
            if pending.len() > 65536 {
                bail!("plugin pending plan too large");
            }
            let _: serde_json::Value = serde_json::from_str(pending)?;
        }
        let row: plugin_installation::ActiveModel = parent.clone().into();
        self.run_serialized_write(|| {
            let row = row.clone();
            async move { plugins::insert_parent(&self.connection, row).await }
        })
        .await
    }
    pub async fn find_plugin_installation(
        &self,
        id: &str,
    ) -> Result<Option<plugin_installation::Model>> {
        plugins::find(&self.connection, id).await
    }
    pub async fn find_plugin_by_upload(
        &self,
        upload: &str,
    ) -> Result<Option<plugin_installation::Model>> {
        plugins::find_by_upload(&self.connection, upload).await
    }
    pub async fn list_plugin_components(&self, id: &str) -> Result<Vec<plugin_component::Model>> {
        plugins::components(&self.connection, id).await
    }
    pub async fn find_skill_plugin_owner(
        &self,
        id: &pioneer_protocol::SkillId,
    ) -> Result<Option<plugin_component::Model>> {
        plugins::owner(&self.connection, "skill", id.as_str()).await
    }
    pub async fn find_mcp_plugin_owner(&self, id: &str) -> Result<Option<plugin_component::Model>> {
        plugins::owner(&self.connection, "mcp", id).await
    }
}

/// Preserve pre-existing standalone names while reserving new plugin names.
/// Ownership is checked separately, before side effects and under the writer.
pub fn validate_standalone_mcp_name(name: &str, existing: bool) -> Result<()> {
    if name.starts_with("pplugin_") && !existing {
        bail!("reserved plugin MCP name");
    }
    Ok(())
}

fn validate_selection(snapshot: &pioneer_protocol::PluginSelectionSnapshot) -> Result<()> {
    use std::collections::HashSet;
    if !matches!(snapshot.phase.as_str(), "prepared" | "ready")
        || snapshot.parents.is_empty()
        || snapshot.parents.len() > 64
        || snapshot.children.len() > 256
    {
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
    fn selection_rejects_unbound_duplicate_and_unbounded_children() {
        use pioneer_protocol::{
            PluginSelectedChild, PluginSelectedParent, PluginSelectionSnapshot,
        };
        let mut snapshot = PluginSelectionSnapshot {
            phase: "prepared".into(),
            parents: vec![PluginSelectedParent {
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
        snapshot.children = vec![snapshot.children[0].clone(); 257];
        assert!(validate_selection(&snapshot).is_err());
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
