use anyhow::{Context, Result, bail};
use pioneer_entity::{plugin_component, plugin_installation};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

pub async fn list<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
) -> Result<Vec<plugin_installation::Model>> {
    let rows = plugin_installation::Entity::find()
        .filter(plugin_installation::Column::WorkspaceId.eq(workspace))
        .order_by_asc(plugin_installation::Column::CreatedAt)
        .limit(1001)
        .all(db)
        .await?;
    if rows.len() > 1000 {
        bail!("plugin inventory limit exceeded");
    }
    Ok(rows)
}

/// Only immediate DB validation/writes run under the caller's writer. The
/// package and bounded pending plan have already been prepared outside it.
pub async fn settle<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    state: &str,
    error: Option<String>,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugin missing")?;
    if parent.revision != revision || parent.state != "installing" {
        bail!("plugin operation changed");
    }
    let pending = parent.pending_json.clone();
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.state = Set(state.to_owned());
    row.pending_json = Set(if state == "installed" { None } else { pending });
    row.last_error = Set(error);
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn record_failure<C: ConnectionTrait>(
    db: &C,
    write: &PluginOwnershipWrite,
    kind: &str,
    diagnostic: &str,
) -> Result<()> {
    let existing = plugin_component::Entity::find_by_id((
        write.plugin_id.clone(),
        kind.to_owned(),
        write.member_key.clone(),
    ))
    .one(db)
    .await?;
    let native_id = existing.as_ref().and_then(|c| {
        if kind == "skill" {
            c.skill_id.as_deref()
        } else {
            c.mcp_installation_id.as_deref()
        }
    });
    validate_publication(
        db,
        Some(write),
        &find(db, &write.plugin_id)
            .await?
            .context("plugin missing")?
            .workspace_id,
        kind,
        &write.child_id,
        native_id,
    )
    .await?;
    let mut link = prepare_link(write, kind);
    if let Some(existing) = existing {
        link.skill_id = Set(existing.skill_id);
        link.mcp_installation_id = Set(existing.mcp_installation_id);
        // Failure is not proof that native assets were updated. Preserve the
        // last committed tree so same-markdown asset repair cannot shortcut.
        link.package_fingerprint = Set(existing.package_fingerprint);
    } else {
        link.skill_id = Set(None);
        link.mcp_installation_id = Set(None);
    }
    link.status = Set("failed".into());
    link.diagnostic = Set(Some(diagnostic.into()));
    publish(db, link).await
}

/// Host-derived ownership accompanying the native child write. The reserved ID
/// is also recorded in the parent's pending plan by the caller before I/O.
#[derive(Clone, Debug)]
pub struct PluginOwnershipWrite {
    pub plugin_id: String,
    pub expected_revision: i64,
    pub member_key: String,
    pub member_path: Option<String>,
    pub package_fingerprint: String,
    pub child_id: String,
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<plugin_installation::Model>> {
    Ok(plugin_installation::Entity::find_by_id(id.to_owned())
        .one(db)
        .await?)
}
pub async fn find_by_upload<C: ConnectionTrait>(
    db: &C,
    upload: &str,
) -> Result<Option<plugin_installation::Model>> {
    Ok(plugin_installation::Entity::find()
        .filter(plugin_installation::Column::SourceUploadId.eq(upload))
        .one(db)
        .await?)
}
pub async fn components<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Vec<plugin_component::Model>> {
    Ok(plugin_component::Entity::find()
        .filter(plugin_component::Column::PluginId.eq(id))
        .order_by_asc(plugin_component::Column::Kind)
        .order_by_asc(plugin_component::Column::MemberKey)
        .all(db)
        .await?)
}
pub async fn owner<C: ConnectionTrait>(
    db: &C,
    kind: &str,
    child_id: &str,
) -> Result<Option<plugin_component::Model>> {
    let column = match kind {
        "skill" => plugin_component::Column::SkillId,
        "mcp" => plugin_component::Column::McpInstallationId,
        _ => bail!("invalid plugin component kind"),
    };
    Ok(plugin_component::Entity::find()
        .filter(column.eq(child_id))
        .one(db)
        .await?)
}

/// Revalidate inside the *native* transaction, before it can change a child.
/// An owned update requires both the same native ID and the same existing link.
/// A new link can only publish a newly reserved native ID, never adopt a row.
pub async fn validate_publication<C: ConnectionTrait>(
    db: &C,
    ownership: Option<&PluginOwnershipWrite>,
    workspace: &str,
    kind: &str,
    child_id: &str,
    existing_id: Option<&str>,
) -> Result<()> {
    let current_owner = owner(db, kind, child_id).await?;
    let Some(write) = ownership else {
        if current_owner.is_some() {
            bail!("plugin-owned child requires its ownership write");
        }
        return Ok(());
    };
    if write.child_id != child_id
        || (kind == "skill" && write.member_key.is_empty())
        || write.member_key.len() > 4096
    {
        bail!("invalid plugin child identity");
    }
    let parent = find(db, &write.plugin_id)
        .await?
        .context("plugin parent not found")?;
    if parent.workspace_id != workspace
        || parent.revision != write.expected_revision
        || !matches!(
            parent.state.as_str(),
            "installing" | "updating" | "interrupted"
        )
    {
        bail!("plugin scope, revision or operation state changed");
    }
    let link = plugin_component::Entity::find_by_id((
        write.plugin_id.clone(),
        kind.to_owned(),
        write.member_key.clone(),
    ))
    .one(db)
    .await?;
    let linked_id = link.as_ref().and_then(|link| match kind {
        "skill" => link.skill_id.as_deref(),
        _ => link.mcp_installation_id.as_deref(),
    });
    if linked_id.is_some_and(|id| id != child_id)
        || existing_id.is_some_and(|id| id != child_id || linked_id != Some(id))
        || current_owner.as_ref().is_some_and(|link| {
            link.plugin_id != write.plugin_id || link.member_key != write.member_key
        })
    {
        bail!("plugin ownership cannot adopt or overwrite another installation");
    }
    // The plan and its links are explicitly bounded; this read never grows
    // with the workspace or database. Recheck the count under the writer.
    if link.is_none() && components(db, &write.plugin_id).await?.len() >= 256 {
        bail!("plugin component limit exceeded");
    }
    Ok(())
}

pub fn prepare_link(write: &PluginOwnershipWrite, kind: &str) -> plugin_component::ActiveModel {
    plugin_component::ActiveModel {
        plugin_id: Set(write.plugin_id.clone()),
        kind: Set(kind.to_owned()),
        member_key: Set(write.member_key.clone()),
        member_path: Set(write.member_path.clone()),
        skill_id: Set((kind == "skill").then(|| write.child_id.clone())),
        mcp_installation_id: Set((kind == "mcp").then(|| write.child_id.clone())),
        package_fingerprint: Set(Some(write.package_fingerprint.clone())),
        status: Set("installed".to_owned()),
        diagnostic: Set(None),
        override_fields_json: Set("[]".to_owned()),
    }
}
pub async fn publish<C: ConnectionTrait>(
    db: &C,
    link: plugin_component::ActiveModel,
) -> Result<()> {
    use sea_orm::sea_query::OnConflict;
    plugin_component::Entity::insert(link)
        .on_conflict(
            OnConflict::columns([
                plugin_component::Column::PluginId,
                plugin_component::Column::Kind,
                plugin_component::Column::MemberKey,
            ])
            .update_columns([
                plugin_component::Column::MemberPath,
                plugin_component::Column::SkillId,
                plugin_component::Column::McpInstallationId,
                plugin_component::Column::PackageFingerprint,
                plugin_component::Column::Status,
                plugin_component::Column::Diagnostic,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn insert_parent<C: ConnectionTrait>(
    db: &C,
    row: plugin_installation::ActiveModel,
) -> Result<()> {
    plugin_installation::Entity::insert(row).exec(db).await?;
    Ok(())
}

pub async fn selection<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<Option<String>> {
    Ok(pioneer_entity::turn::Entity::find_by_id(turn_id)
        .one(db)
        .await?
        .and_then(|t| t.plugin_selection_json))
}
pub async fn set_selection<C: ConnectionTrait>(db: &C, turn_id: &str, value: String) -> Result<()> {
    let row = pioneer_entity::turn::Entity::find_by_id(turn_id)
        .one(db)
        .await?
        .context("turn missing")?;
    let mut row: pioneer_entity::turn::ActiveModel = row.into();
    row.plugin_selection_json = Set(Some(value));
    row.update(db).await?;
    Ok(())
}

/// Bounded native binding existence check for the ready publication transaction.
pub async fn has_skill_binding<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    skill: &str,
) -> Result<bool> {
    use pioneer_entity::turn_skill_binding as binding;
    Ok(binding::Entity::find()
        .filter(binding::Column::TurnId.eq(turn))
        .filter(binding::Column::SkillId.eq(skill))
        .one(db)
        .await?
        .is_some())
}

pub async fn has_mcp_binding<C: ConnectionTrait>(db: &C, turn: &str, server: &str) -> Result<bool> {
    use pioneer_entity::turn_mcp_binding as binding;
    Ok(binding::Entity::find()
        .filter(binding::Column::TurnId.eq(turn))
        .filter(binding::Column::ServerInstallationId.eq(server))
        .one(db)
        .await?
        .is_some())
}

/// Gate publication is an immediate DB operation. Caller owns parent admission;
/// the native operations/FS phase run only after this write has returned.
pub async fn begin_mutation<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    id: &str,
    revision: i64,
    state: &str,
    enabled: bool,
    pending: String,
) -> Result<plugin_installation::Model> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.workspace_id != workspace || parent.revision != revision {
        bail!("plugins.stale");
    }
    if parent.state != "installed" || parent.pending_json.is_some() {
        bail!("plugins.interrupted");
    }
    let next = revision
        .checked_add(1)
        .context("plugin revision exhausted")?;
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.state = Set(state.into());
    row.enabled = Set(enabled);
    row.revision = Set(next);
    row.pending_json = Set(Some(pending));
    row.last_error = Set(None);
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    Ok(row.update(db).await?)
}

pub async fn finish_mutation<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    state: &str,
    error: Option<String>,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision || parent.pending_json.is_none() {
        bail!("plugins.stale");
    }
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.state = Set(state.into());
    if state == "installed" {
        row.pending_json = Set(None);
    }
    row.last_error = Set(error);
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn replace_pending<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    pending: String,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision || parent.pending_json.is_none() || parent.state == "installed" {
        bail!("plugins.stale");
    }
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.pending_json = Set(Some(pending));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn delete_parent<C: ConnectionTrait>(db: &C, id: &str, revision: i64) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision
        || !matches!(parent.state.as_str(), "removing" | "interrupted")
        || components(db, id)
            .await?
            .iter()
            .any(|c| c.skill_id.is_some() || c.mcp_installation_id.is_some())
    {
        bail!("plugin removal is incomplete");
    }
    plugin_component::Entity::delete_many()
        .filter(plugin_component::Column::PluginId.eq(id))
        .exec(db)
        .await?;
    plugin_installation::Entity::delete_by_id(id.to_owned())
        .exec(db)
        .await?;
    Ok(())
}

pub async fn interrupt_unfinished<C: ConnectionTrait>(db: &C) -> Result<()> {
    let parents = plugin_installation::Entity::find()
        .filter(
            Condition::any()
                .add(plugin_installation::Column::State.is_in([
                    "installing",
                    "updating",
                    "removing",
                ]))
                .add(
                    Condition::all()
                        .add(plugin_installation::Column::State.eq("installed"))
                        .add(plugin_installation::Column::PendingJson.is_not_null()),
                ),
        )
        .limit(1001)
        .all(db)
        .await?;
    if parents.len() > 1000 {
        bail!("unfinished plugin inventory limit exceeded");
    }
    for parent in parents {
        let mut row: plugin_installation::ActiveModel = parent.into();
        row.state = Set("interrupted".into());
        row.last_error = Set(Some("plugins.interrupted".into()));
        row.updated_at = Set(chrono::Utc::now().fixed_offset());
        row.update(db).await?;
    }
    Ok(())
}

/// Filter the actual native inventory in bounded batches; no historical scan.
pub async fn workspace_native_threads<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    ids: &[String],
) -> Result<Vec<String>> {
    use pioneer_entity::thread;
    let mut result = Vec::new();
    for batch in ids.chunks(128) {
        let rows = thread::Entity::find()
            .filter(thread::Column::WorkspaceId.eq(workspace))
            .filter(thread::Column::Id.is_in(batch.iter().cloned()))
            .all(db)
            .await?;
        result.extend(rows.into_iter().map(|row| row.id));
    }
    Ok(result)
}

pub async fn interrupt_mutation<C: ConnectionTrait>(
    db: &C,
    id: &str,
    revision: i64,
    pending: String,
    error: String,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision {
        bail!("plugins.stale");
    }
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.state = Set("interrupted".into());
    row.pending_json = Set(Some(pending));
    row.last_error = Set(Some(error));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

/// Allow the native reload after all child/file work, retaining the execution
/// fence until the caller acknowledges reload and clears pending atomically.
pub async fn prepare_reload<C: ConnectionTrait>(db: &C, id: &str, revision: i64) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision
        || parent.pending_json.is_none()
        || !matches!(
            parent.state.as_str(),
            "installing" | "updating" | "interrupted" | "installed"
        )
    {
        bail!("plugins.stale");
    }
    let mut row: plugin_installation::ActiveModel = parent.into();
    row.state = Set("installed".into());
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

/// Queued work graphs can outlive their root actor. Only exact active native
/// graph roots carrying this trusted parent selection are candidates; terminal
/// history and unrelated workspace graphs are excluded by the existing scope.
pub async fn graph_stop_candidates<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    parent: &str,
) -> Result<Vec<(String, String)>> {
    use sea_orm::{DbBackend, Statement};
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT t.thread_id AS thread_id,t.id AS turn_id FROM agent_execution e \
         JOIN agent_work_resource_scope s ON s.root_execution_id=e.id \
         JOIN agent_turn_response_execution r ON r.execution_id=e.id \
         JOIN turn t ON t.id=r.turn_id JOIN thread th ON th.id=t.thread_id \
         WHERE e.workspace_id=? AND th.workspace_id=? AND e.parent_execution_id IS NULL \
         AND e.work_graph_root_execution_id=e.id AND s.status='active' \
         AND EXISTS (SELECT 1 FROM json_each(t.plugin_selection_json,'$.parents') p \
                     WHERE json_extract(p.value,'$.id')=?) LIMIT 1001",
            vec![workspace.into(), workspace.into(), parent.into()],
        ))
        .await?;
    if rows.len() > 1000 {
        bail!("plugin graph inventory limit exceeded");
    }
    rows.iter()
        .map(|row| Ok((row.try_get("", "thread_id")?, row.try_get("", "turn_id")?)))
        .collect()
}

/// One admitted native settings write. Only field names and an opaque current
/// action outcome live on the parent/link; settings remain in native records.
#[derive(Clone, Debug)]
pub struct PluginNativeWrite {
    pub plugin_id: String,
    pub expected_revision: i64,
    pub member_key: String,
    pub child_id: String,
    pub override_fields_json: String,
    pub pending_after: String,
}

pub async fn validate_native_write<C: ConnectionTrait>(
    db: &C,
    write: Option<&PluginNativeWrite>,
    workspace: &str,
    kind: &str,
    child: &str,
) -> Result<()> {
    let link = owner(db, kind, child).await?;
    match (write, link) {
        (None, None) => Ok(()),
        (Some(write), Some(link)) => {
            let parent = find(db, &write.plugin_id)
                .await?
                .context("plugin parent missing")?;
            if write.child_id != child
                || link.plugin_id != write.plugin_id
                || link.member_key != write.member_key
                || parent.workspace_id != workspace
                || parent.revision != write.expected_revision
                || parent.pending_json.is_none()
                || !matches!(
                    parent.state.as_str(),
                    "updating" | "removing" | "interrupted"
                )
            {
                bail!("plugin native settings admission changed");
            }
            Ok(())
        }
        _ => bail!("plugin-owned native mutation requires parent admission"),
    }
}

pub async fn publish_native_write<C: ConnectionTrait>(
    db: &C,
    write: &PluginNativeWrite,
    kind: &str,
    removed: bool,
) -> Result<()> {
    let link = plugin_component::Entity::find_by_id((
        write.plugin_id.clone(),
        kind.to_owned(),
        write.member_key.clone(),
    ))
    .one(db)
    .await?
    .context("plugin component missing")?;
    let mut row: plugin_component::ActiveModel = link.into();
    row.override_fields_json = Set(write.override_fields_json.clone());
    if removed {
        row.status = Set("removed_by_user".into());
        row.diagnostic = Set(None);
    }
    row.update(db).await?;
    let parent = find(db, &write.plugin_id)
        .await?
        .context("plugin parent missing")?;
    if parent.revision != write.expected_revision {
        bail!("plugin settings revision changed");
    }
    let mut parent: plugin_installation::ActiveModel = parent.into();
    parent.pending_json = Set(Some(write.pending_after.clone()));
    parent.updated_at = Set(chrono::Utc::now().fixed_offset());
    parent.update(db).await?;
    Ok(())
}
