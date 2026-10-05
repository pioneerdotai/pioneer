use anyhow::{Context, Result, bail};
use pioneer_entity::{plugin_component, plugin_installation};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
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
