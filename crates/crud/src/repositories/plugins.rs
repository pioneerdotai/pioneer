use anyhow::{Context, Result, bail};
use pioneer_entity::{plugin_component, plugin_installation};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

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
