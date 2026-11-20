use crate::PackageRecord;
use anyhow::{Context, Result, bail};
use pioneer_entity::{
    app_installation, package_component, package_installation, plugin_installation,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, ModelTrait,
    QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn list<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    kind: crate::PackageKind,
) -> Result<Vec<PackageRecord>> {
    let rows = package_installation::Entity::find()
        .filter(package_installation::Column::WorkspaceId.eq(workspace))
        .filter(package_installation::Column::Kind.eq(kind.as_str()))
        .order_by_asc(package_installation::Column::CreatedAt)
        .all(db)
        .await?;
    project_rows(db, rows).await
}
pub(super) async fn project_rows<C: ConnectionTrait>(
    db: &C,
    rows: Vec<package_installation::Model>,
) -> Result<Vec<PackageRecord>> {
    let mut result = Vec::with_capacity(rows.len());
    for chunk in rows.chunks(64) {
        let ids: Vec<_> = chunk.iter().map(|p| p.id.clone()).collect();
        let plugins = plugin_installation::Entity::find()
            .filter(plugin_installation::Column::Id.is_in(ids.clone()))
            .all(db)
            .await?;
        let apps = app_installation::Entity::find()
            .filter(app_installation::Column::Id.is_in(ids))
            .all(db)
            .await?;
        for parent in chunk {
            result.push(PackageRecord::project(
                parent.clone(),
                plugins.iter().find(|p| p.id == parent.id).cloned(),
                apps.iter().find(|p| p.id == parent.id).cloned(),
            )?);
        }
    }
    Ok(result)
}
/// Bounded native-catalog ownership projection; no per-skill reader roundtrips.
pub async fn skill_owners<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
) -> Result<Vec<(package_component::Model, Option<PackageRecord>)>> {
    let mut result = Vec::new();
    for chunk in ids.chunks(128) {
        let links = package_component::Entity::find()
            .filter(package_component::Column::Kind.eq("skill"))
            .filter(package_component::Column::SkillId.is_in(chunk.iter().cloned()))
            .all(db)
            .await?;
        let parent_ids = links
            .iter()
            .map(|l| l.package_id.clone())
            .collect::<Vec<_>>();
        let parents = package_installation::Entity::find()
            .filter(package_installation::Column::Id.is_in(parent_ids))
            .all(db)
            .await?;
        let parents = project_rows(db, parents).await?;
        anyhow::ensure!(
            links
                .iter()
                .all(|link| parents.iter().any(|parent| parent.id == link.package_id)),
            "package owner missing"
        );
        result.extend(links.into_iter().map(|link| {
            let parent = parents.iter().find(|p| p.id == link.package_id).cloned();
            (link, parent)
        }));
    }
    Ok(result)
}

/// Transitions require a writer transaction: reader validation and its write
/// must not be separated by another dispatched transition. Only immediate
/// DB validation/writes run under the caller's writer. The
/// package and bounded pending plan have already been prepared outside it.
pub async fn settle(
    db: &pioneer_sqlite::SqliteTransaction,
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
    let mut row: package_installation::ActiveModel = parent.operational();
    row.state = Set(state.to_owned());
    row.pending_json = Set(if state == "installed" { None } else { pending });
    row.last_error_json = Set(error
        .as_deref()
        .map(crate::package_record::encode_operation_error));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn record_failure(
    db: &pioneer_sqlite::SqliteTransaction,
    write: &PluginOwnershipWrite,
    kind: &str,
    diagnostic: &str,
) -> Result<()> {
    let existing = package_component::Entity::find_by_id((
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
pub type PluginOwnershipWrite = PackageOwnershipWrite;
#[derive(Clone, Debug)]
pub struct PackageOwnershipWrite {
    pub kind: crate::PackageKind,
    pub plugin_id: String,
    pub expected_revision: i64,
    pub member_key: String,
    pub member_path: Option<String>,
    pub package_fingerprint: String,
    pub child_id: String,
}

pub async fn find<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<PackageRecord>> {
    let row = package_installation::Entity::find_by_id(id.to_owned())
        .one(db)
        .await?;
    Ok(project_rows(db, row.into_iter().collect()).await?.pop())
}
pub async fn find_by_upload<C: ConnectionTrait>(
    db: &C,
    upload: &str,
) -> Result<Option<PackageRecord>> {
    let rows = package_installation::Entity::find()
        .filter(package_installation::Column::SourceUploadId.eq(upload))
        .all(db)
        .await?;
    Ok(project_rows(db, rows).await?.pop())
}
pub async fn components<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Vec<package_component::Model>> {
    Ok(package_component::Entity::find()
        .filter(package_component::Column::PackageId.eq(id))
        .order_by_asc(package_component::Column::Kind)
        .order_by_asc(package_component::Column::MemberKey)
        .all(db)
        .await?)
}
/// Fetch list inventory in bounded batches rather than one request per parent.
pub async fn components_for_parents<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
) -> Result<Vec<package_component::Model>> {
    let mut rows = Vec::new();
    for parents in ids.chunks(16) {
        let batch = package_component::Entity::find()
            .filter(package_component::Column::PackageId.is_in(parents.iter().cloned()))
            .order_by_asc(package_component::Column::Kind)
            .order_by_asc(package_component::Column::MemberKey)
            .all(db)
            .await?;
        rows.extend(batch);
    }
    Ok(rows)
}

pub async fn owner<C: ConnectionTrait>(
    db: &C,
    kind: &str,
    child_id: &str,
) -> Result<Option<package_component::Model>> {
    let column = match kind {
        "skill" => package_component::Column::SkillId,
        "mcp" => package_component::Column::McpInstallationId,
        _ => bail!("invalid plugin component kind"),
    };
    Ok(package_component::Entity::find()
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
    if parent.kind != write.kind
        || parent.workspace_id != workspace
        || parent.revision != write.expected_revision
        || !matches!(
            parent.state.as_str(),
            "installing" | "updating" | "interrupted"
        )
    {
        bail!("plugin scope, revision or operation state changed");
    }
    let link = package_component::Entity::find_by_id((
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
            link.package_id != write.plugin_id || link.member_key != write.member_key
        })
    {
        bail!("plugin ownership cannot adopt or overwrite another installation");
    }
    Ok(())
}

pub fn prepare_link(write: &PluginOwnershipWrite, kind: &str) -> package_component::ActiveModel {
    package_component::ActiveModel {
        package_id: Set(write.plugin_id.clone()),
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
    link: package_component::ActiveModel,
) -> Result<()> {
    use sea_orm::sea_query::OnConflict;
    package_component::Entity::insert(link)
        .on_conflict(
            OnConflict::columns([
                package_component::Column::PackageId,
                package_component::Column::Kind,
                package_component::Column::MemberKey,
            ])
            .update_columns([
                package_component::Column::MemberPath,
                package_component::Column::SkillId,
                package_component::Column::McpInstallationId,
                package_component::Column::PackageFingerprint,
                package_component::Column::Status,
                package_component::Column::Diagnostic,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn insert_parent(
    db: &pioneer_sqlite::SqliteTransaction,
    row: &PackageRecord,
    metadata: Option<super::plugin_metadata::PreparedMetadata>,
) -> Result<()> {
    if row.kind == crate::PackageKind::App {
        anyhow::ensure!(
            app_installation::Entity::find()
                .inner_join(package_installation::Entity)
                .filter(app_installation::Column::Slug.eq(&row.name))
                .filter(package_installation::Column::WorkspaceId.eq(&row.workspace_id))
                .filter(if row.source_upload_id.starts_with("dev:") {
                    package_installation::Column::SourceUploadId.eq(&row.source_upload_id)
                } else {
                    package_installation::Column::SourceUploadId.not_like("dev:%")
                })
                .one(db)
                .await?
                .is_none(),
            "apps.slug_conflict"
        );
    }
    package_installation::Entity::insert(row.operational())
        .exec(db)
        .await?;
    match row.kind {
        crate::PackageKind::Plugin => {
            let mut extension = row.plugin_metadata();
            if let Some(ref prepared) = metadata {
                super::plugin_metadata::apply_columns(&mut extension, prepared.columns.clone());
            }
            plugin_installation::Entity::insert(extension)
                .exec(db)
                .await?;
            if let Some(prepared) = metadata {
                super::plugin_metadata::replace_lists(
                    db,
                    &row.id,
                    prepared.keywords,
                    prepared.prompts,
                )
                .await?;
            }
        }
        crate::PackageKind::App => {
            anyhow::ensure!(
                metadata.is_none(),
                "App cannot contain Plugin metadata storage"
            );
            app_installation::Entity::insert(app_installation::ActiveModel {
                id: Set(row.id.clone()),
                slug: Set(row.name.clone()),
                manifest_json: Set(row
                    .app_manifest_json
                    .clone()
                    .context("App manifest missing")?),
                localizations_json: Set(row
                    .app_localizations_json
                    .clone()
                    .context("App locales missing")?),
                ..Default::default()
            })
            .exec(db)
            .await?;
        }
    }
    Ok(())
}

pub async fn selection<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<Option<String>> {
    Ok(pioneer_entity::turn::Entity::find_by_id(turn_id)
        .one(db)
        .await?
        .and_then(|t| t.package_selection_json))
}
pub async fn set_selection<C: ConnectionTrait>(db: &C, turn_id: &str, value: String) -> Result<()> {
    let row = pioneer_entity::turn::Entity::find_by_id(turn_id)
        .one(db)
        .await?
        .context("turn missing")?;
    let mut row: pioneer_entity::turn::ActiveModel = row.into();
    row.package_selection_json = Set(Some(value));
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
pub async fn begin_mutation(
    db: &pioneer_sqlite::SqliteTransaction,
    workspace: &str,
    id: &str,
    revision: i64,
    state: &str,
    enabled: bool,
    pending: String,
    allow_implicit_invocation: Option<bool>,
) -> Result<PackageRecord> {
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
    let mut row: package_installation::ActiveModel = parent.operational();
    row.state = Set(state.into());
    row.enabled = Set(enabled);
    if let Some(implicit) = allow_implicit_invocation {
        row.allow_implicit_invocation = Set(implicit);
    }
    row.revision = Set(next);
    row.pending_json = Set(Some(pending));
    row.last_error_json = Set(None);
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    find(db, id)
        .await?
        .context("package missing after mutation")
}

pub async fn finish_mutation(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
    state: &str,
    error: Option<String>,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision || parent.pending_json.is_none() {
        bail!("plugins.stale");
    }
    let mut row: package_installation::ActiveModel = parent.operational();
    row.state = Set(state.into());
    if state == "installed" {
        row.pending_json = Set(None);
    }
    row.last_error_json = Set(error
        .as_deref()
        .map(crate::package_record::encode_operation_error));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn replace_pending(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
    pending: String,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision || parent.pending_json.is_none() || parent.state == "installed" {
        bail!("plugins.stale");
    }
    let mut row: package_installation::ActiveModel = parent.operational();
    row.pending_json = Set(Some(pending));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

pub async fn delete_parent(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
) -> Result<()> {
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
    package_component::Entity::delete_many()
        .filter(package_component::Column::PackageId.eq(id))
        .exec(db)
        .await?;
    super::plugin_metadata::replace_lists(db, id, Vec::new(), Vec::new()).await?;
    package_installation::Entity::delete_by_id(id.to_owned())
        .exec(db)
        .await?;
    Ok(())
}

pub async fn interrupt_unfinished(db: &pioneer_sqlite::SqliteTransaction) -> Result<usize> {
    let parents = package_installation::Entity::find()
        .filter(
            Condition::any()
                .add(package_installation::Column::State.is_in([
                    "installing",
                    "updating",
                    "removing",
                ]))
                .add(
                    Condition::all()
                        .add(package_installation::Column::State.eq("installed"))
                        .add(package_installation::Column::PendingJson.is_not_null()),
                ),
        )
        .limit(128)
        .all(db)
        .await?;
    let count = parents.len();
    for parent in parents {
        let mut row: package_installation::ActiveModel = parent.into();
        row.state = Set("interrupted".into());
        row.last_error_json = Set(Some(crate::package_record::encode_operation_error(
            "plugins.interrupted",
        )));
        row.updated_at = Set(chrono::Utc::now().fixed_offset());
        row.update(db).await?;
    }
    Ok(count)
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

pub async fn interrupt_mutation(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
    pending: String,
    error: String,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision {
        bail!("plugins.stale");
    }
    let mut row: package_installation::ActiveModel = parent.operational();
    row.state = Set("interrupted".into());
    row.pending_json = Set(Some(pending));
    row.last_error_json = Set(Some(crate::package_record::encode_operation_error(&error)));
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}

/// Allow the native reload after all child/file work, retaining the execution
/// fence until the caller acknowledges reload and clears pending atomically.
pub async fn prepare_reload(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
) -> Result<()> {
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
    let mut row: package_installation::ActiveModel = parent.operational();
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
    use pioneer_entity::{
        agent_execution as execution, agent_turn_response_execution as response,
        agent_work_resource_scope as scope, thread, turn,
    };
    use sea_orm::{
        FromQueryResult, JoinType,
        sea_query::{Alias, Expr, ExprTrait, Func, Query},
    };
    #[derive(FromQueryResult)]
    struct Candidate {
        thread_id: String,
        turn_id: String,
    }
    let parents = Query::select()
        .expr(Expr::val(1))
        .from_function(
            Func::cust(Alias::new("json_each"))
                .arg(Expr::col((
                    turn::Entity,
                    turn::Column::PackageSelectionJson,
                )))
                .arg(Expr::val("$.parents")),
            Alias::new("p"),
        )
        .and_where(
            Func::cust(Alias::new("json_extract"))
                .arg(Expr::col((Alias::new("p"), Alias::new("value"))))
                .arg(Expr::val("$.id"))
                .eq(parent),
        )
        .to_owned();
    let rows = execution::Entity::find()
        .select_only()
        .expr_as(
            Expr::col((turn::Entity, turn::Column::ThreadId)),
            "thread_id",
        )
        .expr_as(Expr::col((turn::Entity, turn::Column::Id)), "turn_id")
        .join_rev(
            JoinType::InnerJoin,
            scope::Entity::belongs_to(execution::Entity)
                .from(scope::Column::RootExecutionId)
                .to(execution::Column::Id)
                .into(),
        )
        .join_rev(
            JoinType::InnerJoin,
            response::Entity::belongs_to(execution::Entity)
                .from(response::Column::ExecutionId)
                .to(execution::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            response::Entity::belongs_to(turn::Entity)
                .from(response::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(execution::Column::WorkspaceId.eq(workspace))
        .filter(thread::Column::WorkspaceId.eq(workspace))
        .filter(execution::Column::ParentExecutionId.is_null())
        .filter(
            Expr::col((
                execution::Entity,
                execution::Column::WorkGraphRootExecutionId,
            ))
            .eq(Expr::col((execution::Entity, execution::Column::Id))),
        )
        .filter(scope::Column::Status.eq("active"))
        .filter(Expr::exists(parents))
        .into_model::<Candidate>()
        .all(db)
        .await?;
    Ok(rows.into_iter().map(|r| (r.thread_id, r.turn_id)).collect())
}

/// One admitted native settings write. Only field names and an opaque current
/// action outcome live on the parent/link; settings remain in native records.
pub type PluginNativeWrite = PackageNativeWrite;
#[derive(Clone, Debug)]
pub struct PackageNativeWrite {
    pub kind: crate::PackageKind,
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
                || link.package_id != write.plugin_id
                || link.member_key != write.member_key
                || parent.kind != write.kind
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

pub async fn publish_native_write(
    db: &pioneer_sqlite::SqliteTransaction,
    write: &PluginNativeWrite,
    kind: &str,
    removed: bool,
) -> Result<()> {
    let link = package_component::Entity::find_by_id((
        write.plugin_id.clone(),
        kind.to_owned(),
        write.member_key.clone(),
    ))
    .one(db)
    .await?
    .context("plugin component missing")?;
    let mut row: package_component::ActiveModel = link.into();
    row.override_fields_json = Set(write.override_fields_json.clone());
    if removed {
        row.status = Set("removed_by_user".into());
        row.diagnostic = Set(None);
    }
    row.update(db).await?;
    let parent = find(db, &write.plugin_id)
        .await?
        .context("plugin parent missing")?;
    if parent.kind != write.kind || parent.revision != write.expected_revision {
        bail!("plugin settings revision changed");
    }
    let mut parent: package_installation::ActiveModel = parent.operational();
    parent.pending_json = Set(Some(write.pending_after.clone()));
    parent.updated_at = Set(chrono::Utc::now().fixed_offset());
    parent.update(db).await?;
    Ok(())
}

/// One reader snapshot protects native and pending credential owners.
/// Both arms originate in the Entity projection path. UNION ALL preserves the
/// single snapshot when a credential moves between owners during publication.
pub async fn credential_retention<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<(String, String, String)>> {
    use pioneer_entity::mcp_server_installation as native;
    use sea_orm::{
        QueryTrait,
        sea_query::{Expr, UnionType},
    };
    let pending = package_installation::Entity::find()
        .select_only()
        .expr_as(Expr::val("pending"), "kind")
        .column_as(package_installation::Column::Id, "owner")
        .column_as(package_installation::Column::PendingJson, "payload")
        .filter(package_installation::Column::PendingJson.is_not_null())
        .into_query();
    let mut query = native::Entity::find()
        .select_only()
        .expr_as(Expr::val("native"), "kind")
        .column_as(native::Column::Id, "owner")
        .column_as(native::Column::SecretRefsJson, "payload")
        .into_query();
    query.union(UnionType::All, pending);
    let rows = db.query_all(&query).await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("", "kind")?,
                row.try_get("", "owner")?,
                row.try_get("", "payload")?,
            ))
        })
        .collect()
}

pub async fn resume_mutation(
    db: &pioneer_sqlite::SqliteTransaction,
    workspace: &str,
    id: &str,
    revision: i64,
    expected_pending: &str,
    pending: &str,
    state: &str,
) -> Result<PackageRecord> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.workspace_id != workspace
        || parent.revision != revision
        || parent.pending_json.as_deref() != Some(expected_pending)
    {
        bail!("plugins.stale");
    }
    let next = revision
        .checked_add(1)
        .context("plugin revision exhausted")?;
    let mut row: package_installation::ActiveModel = parent.operational();
    row.revision = Set(next);
    row.state = Set(state.to_owned());
    row.pending_json = Set(Some(pending.to_owned()));
    row.last_error_json = Set(None);
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    find(db, id)
        .await?
        .context("package missing after mutation")
}
pub(crate) async fn publish_package(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
    name: &str,
    version: Option<&str>,
    fingerprint: &str,
    upload_id: Option<&str>,
    metadata: Option<super::plugin_metadata::PreparedMetadata>,
    app: Option<&crate::PreparedAppMetadata>,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision || parent.pending_json.is_none() || parent.state != "updating" {
        bail!("plugins.stale");
    }
    match (parent.kind, app) {
        (crate::PackageKind::Plugin, None) => {}
        (crate::PackageKind::App, Some(app))
            if parent.name == app.slug && app.fingerprint == fingerprint && metadata.is_none() => {}
        _ => bail!("package metadata domain or App identity changed"),
    }
    let mut row: package_installation::ActiveModel = parent.operational();
    if let Some(upload) = upload_id {
        row.source_upload_id = Set(upload.into());
    }
    if parent.kind == crate::PackageKind::Plugin {
        let mut extension = parent.plugin_metadata();
        extension.name = Set(name.into());
        extension.version = Set(version.map(str::to_owned));
        if let Some(metadata) = metadata {
            super::plugin_metadata::apply_columns(&mut extension, metadata.columns);
            super::plugin_metadata::replace_lists(db, id, metadata.keywords, metadata.prompts)
                .await?;
        }
        extension.update(db).await?;
    }
    if let Some(app) = app {
        let extension = app_installation::Entity::find_by_id(id.to_owned())
            .one(db)
            .await?
            .context("App extension missing")?;
        let mut extension: app_installation::ActiveModel = extension.into();
        extension.manifest_json = Set(app.manifest_json.clone());
        extension.localizations_json = Set(app.localizations_json.clone());
        extension.update(db).await?;
    }
    row.package_fingerprint = Set(fingerprint.into());
    row.updated_at = Set(chrono::Utc::now().fixed_offset());
    row.update(db).await?;
    Ok(())
}
pub async fn forget_component(
    db: &pioneer_sqlite::SqliteTransaction,
    id: &str,
    revision: i64,
    kind: &str,
    key: &str,
) -> Result<()> {
    let parent = find(db, id).await?.context("plugins.not_found")?;
    if parent.revision != revision
        || parent.pending_json.is_none()
        || !matches!(
            parent.state.as_str(),
            "updating" | "removing" | "interrupted"
        )
    {
        bail!("plugins.stale");
    }
    if let Some(row) =
        package_component::Entity::find_by_id((id.to_owned(), kind.to_owned(), key.to_owned()))
            .one(db)
            .await?
    {
        if row.skill_id.is_some() || row.mcp_installation_id.is_some() {
            bail!("native component must be removed first");
        }
        row.delete(db).await?;
    }
    Ok(())
}

/// A package removal interrupted before forgetting its link is a repairable
/// failure, not a user restriction. Reuse the existing status/diagnostic fields.
pub async fn mark_package_removed(
    db: &pioneer_sqlite::SqliteTransaction,
    write: &PluginNativeWrite,
    kind: &str,
) -> Result<()> {
    let link = package_component::Entity::find_by_id((
        write.plugin_id.clone(),
        kind.to_owned(),
        write.member_key.clone(),
    ))
    .one(db)
    .await?
    .context("plugin component missing")?;
    if link.skill_id.is_some() || link.mcp_installation_id.is_some() {
        bail!("native removal not published");
    }
    let mut row: package_component::ActiveModel = link.into();
    row.status = Set("failed".into());
    row.diagnostic = Set(Some("component_removed_for_update".into()));
    row.update(db).await?;
    Ok(())
}

/// A finite startup frontier for the App runtime owner, including disabled parents
/// so interrupted executions can be fenced before a new execution is admitted.
pub async fn app_runtime_page<C: ConnectionTrait>(
    db: &C,
    after: &str,
    through: &str,
    limit: u64,
) -> Result<Vec<(package_installation::Model, Option<app_installation::Model>)>> {
    let rows = package_installation::Entity::find()
        .filter(package_installation::Column::Kind.eq("app"))
        .filter(package_installation::Column::Id.gt(after))
        .filter(package_installation::Column::Id.lte(through))
        .order_by_asc(package_installation::Column::Id)
        .limit(limit.min(64))
        .all(db)
        .await?;
    let ids: Vec<_> = rows.iter().map(|r| r.id.clone()).collect();
    let apps = app_installation::Entity::find()
        .filter(app_installation::Column::Id.is_in(ids))
        .all(db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|parent| {
            let app = apps.iter().find(|a| a.id == parent.id).cloned();
            (parent, app)
        })
        .collect())
}

/// Capture the finite App discovery frontier before paging a maintenance pass.
pub async fn app_runtime_frontier<C: ConnectionTrait>(db: &C) -> Result<Option<String>> {
    Ok(package_installation::Entity::find()
        .select_only()
        .column(package_installation::Column::Id)
        .filter(package_installation::Column::Kind.eq("app"))
        .order_by_desc(package_installation::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?)
}
