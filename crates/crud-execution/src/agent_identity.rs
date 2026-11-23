//! Durable runtime agent identities and presentation snapshots.

use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud_access::actor_nickname::{NICKNAME_OWNER_AGENT, claim_actor_nickname};
use pioneer_entity::{
    agent_execution, agent_identity, agent_presentation_snapshot, agent_turn_response_execution,
    native_agent_config,
};
use pioneer_protocol::{
    AgentExecutionId, AgentIdentityId, AgentIdentityProjection, AgentIdentitySourceKind,
    AgentPresentationSnapshot,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use std::collections::BTreeMap;

pub const AGENT_PROJECTION_BATCH_LIMIT: usize = 200;

pub const SOURCE_NATIVE_AGENT: &str = "native_agent";
pub const SOURCE_CLI_RUNTIME_INSTANCE: &str = "cli_runtime_instance";
pub const SOURCE_EPHEMERAL: &str = "ephemeral";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAgentConfigInput {
    pub id: String,
    pub workspace_id: String,
    pub system_key: Option<String>,
    pub display_name: String,
    pub nickname: String,
    pub enabled: bool,
    pub avatar_revision: Option<String>,
    pub config_revision: i64,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIdentityInput {
    pub id: String,
    pub workspace_id: String,
    pub source_kind: String,
    pub source_id: String,
    pub source_revision: i64,
    pub source_fingerprint: String,
    pub now: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentationSnapshotInput {
    pub id: String,
    pub agent_identity_id: String,
    pub source_revision: i64,
    pub source_fingerprint: String,
    pub display_name: String,
    pub nickname: String,
    pub avatar_revision: Option<String>,
    pub role_label: Option<String>,
    pub now: DateTimeWithTimeZone,
}

pub async fn load_native_agent_config<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<native_agent_config::Model>> {
    native_agent_config::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to load native agent config")
}

pub async fn load_native_agent_config_by_system_key<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    system_key: &str,
) -> Result<Option<native_agent_config::Model>> {
    native_agent_config::Entity::find()
        .filter(native_agent_config::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(native_agent_config::Column::SystemKey.eq(system_key.to_owned()))
        .one(db)
        .await
        .context("failed to load native agent config by exact system key")
}

pub async fn ensure_native_agent_config<C: ConnectionTrait>(
    db: &C,
    input: &NativeAgentConfigInput,
) -> Result<native_agent_config::Model> {
    native_agent_config::Entity::insert(native_agent_config::ActiveModel {
        id: Set(input.id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        system_key: Set(input.system_key.clone()),
        display_name: Set(input.display_name.clone()),
        nickname: Set(input.nickname.clone()),
        enabled: Set(input.enabled),
        avatar_revision: Set(input.avatar_revision.clone()),
        config_revision: Set(input.config_revision),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(native_agent_config::Column::Id)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist native agent config")?;
    let config = load_native_agent_config(db, &input.id)
        .await?
        .context("native agent config disappeared after idempotent insert")?;
    if config.workspace_id != input.workspace_id
        || config.system_key != input.system_key
        || config.display_name != input.display_name
        || config.nickname != input.nickname
        || config.enabled != input.enabled
        || config.avatar_revision != input.avatar_revision
        || config.config_revision != input.config_revision
    {
        bail!("native agent config id was reused with different immutable facts");
    }
    Ok(config)
}

pub async fn load_agent_identity_by_source<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::SourceKind.eq(source_kind.to_owned()))
        .filter(agent_identity::Column::SourceId.eq(source_id.to_owned()))
        .one(db)
        .await
        .context("failed to resolve agent identity by exact source")
}

pub async fn load_agent_identity<C: ConnectionTrait>(
    db: &C,
    identity_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find_by_id(identity_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent identity")
}

pub async fn load_active_agent_identity<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    identity_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find_by_id(identity_id.to_owned())
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null())
        .one(db)
        .await
        .context("failed to load exact active workspace agent identity")
}

pub async fn load_active_agent_identity_by_source<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_kind: &str,
    source_id: &str,
) -> Result<Option<agent_identity::Model>> {
    agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::SourceKind.eq(source_kind.to_owned()))
        .filter(agent_identity::Column::SourceId.eq(source_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null())
        .one(db)
        .await
        .context("failed to resolve exact active agent identity source")
}

pub async fn list_active_agent_identities_page<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    after: Option<&str>,
) -> Result<Vec<agent_identity::Model>> {
    let mut query = agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::Status.eq("active"))
        .filter(agent_identity::Column::RetiredAt.is_null());
    if let Some(after) = after {
        query = query.filter(agent_identity::Column::Id.gt(after));
    }
    query
        .order_by_asc(agent_identity::Column::Id)
        .limit(AGENT_PROJECTION_BATCH_LIMIT as u64)
        .all(db)
        .await
        .context("failed to list active workspace agent identities")
}

pub async fn load_agent_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot_id: &str,
) -> Result<Option<agent_presentation_snapshot::Model>> {
    agent_presentation_snapshot::Entity::find_by_id(snapshot_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent presentation snapshot")
}

pub async fn load_current_agent_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    identity_id: &str,
    source_revision: i64,
    source_fingerprint: &str,
) -> Result<Option<agent_presentation_snapshot::Model>> {
    agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(identity_id.to_owned()))
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(source_fingerprint.to_owned()),
        )
        .order_by_desc(agent_presentation_snapshot::Column::CreatedAt)
        .order_by_desc(agent_presentation_snapshot::Column::Id)
        .one(db)
        .await
        .context("failed to load current agent presentation snapshot")
}

/// Converts the three normalized persistence rows into the one immutable
/// presentation contract consumed by conversation clients.  Mutable current
/// identity presentation is deliberately ignored; only source kind comes from
/// the identity row because it is not duplicated into the snapshot table.
pub fn agent_presentation_snapshot_from_rows(
    identity: &agent_identity::Model,
    execution: &agent_execution::Model,
    snapshot: &agent_presentation_snapshot::Model,
) -> Result<AgentPresentationSnapshot> {
    if execution.agent_identity_id != identity.id
        || snapshot.agent_identity_id != identity.id
        || execution.presentation_snapshot_id.as_deref() != Some(snapshot.id.as_str())
        || execution.identity_source_revision != snapshot.source_revision
        || execution.identity_source_fingerprint != snapshot.source_fingerprint
    {
        bail!("AgentExecution presentation rows do not form one exact immutable snapshot");
    }
    let source_kind = agent_identity_source_kind(identity.source_kind.as_str())?;
    let source_revision = u64::try_from(snapshot.source_revision)
        .context("Agent presentation source revision is negative")?;
    if source_revision == 0 {
        bail!("Agent presentation source revision must be positive");
    }
    let agent_identity_id = AgentIdentityId::new(identity.id.clone())
        .map_err(|error| anyhow!("AgentIdentity id is invalid: {error:?}"))?;
    let projection = AgentIdentityProjection::new(
        agent_identity_id.clone(),
        source_kind,
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        snapshot.avatar_revision.clone(),
        snapshot.role_label.clone(),
        source_revision,
        snapshot.source_fingerprint.clone(),
    )
    .map_err(|error| anyhow!("Agent presentation snapshot is invalid: {error:?}"))?;
    Ok(AgentPresentationSnapshot {
        agent_identity_id,
        agent_execution_id: AgentExecutionId::new(execution.id.clone())
            .map_err(|error| anyhow!("AgentExecution id is invalid: {error:?}"))?,
        identity_source_kind: projection.source_kind,
        identity_source_revision: projection.source_revision,
        display_name: projection.display_name,
        nickname: projection.nickname,
        avatar_revision: projection.avatar_revision,
        role_label: projection.role_label,
    })
}

fn current_agent_identity_projection_from_rows(
    identity: &agent_identity::Model,
    snapshot: &agent_presentation_snapshot::Model,
) -> Result<AgentIdentityProjection> {
    if snapshot.agent_identity_id != identity.id
        || snapshot.source_revision != identity.source_revision
        || snapshot.source_fingerprint != identity.source_fingerprint
    {
        bail!("Agent timeline presentation rows do not form the current identity version");
    }
    let source_revision = u64::try_from(snapshot.source_revision)
        .context("Agent presentation source revision is negative")?;
    if source_revision == 0 {
        bail!("Agent presentation source revision must be positive");
    }
    AgentIdentityProjection::new(
        AgentIdentityId::new(identity.id.clone())
            .map_err(|error| anyhow!("AgentIdentity id is invalid: {error:?}"))?,
        agent_identity_source_kind(identity.source_kind.as_str())?,
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        snapshot.avatar_revision.clone(),
        snapshot.role_label.clone(),
        source_revision,
        snapshot.source_fingerprint.clone(),
    )
    .map_err(|error| anyhow!("current Agent presentation is invalid: {error:?}"))
}

fn current_agent_author_from_rows(
    identity: &agent_identity::Model,
    execution: &agent_execution::Model,
    immutable_snapshot: &agent_presentation_snapshot::Model,
    current_snapshot: &agent_presentation_snapshot::Model,
) -> Result<pioneer_protocol::TurnAuthorSnapshot> {
    let immutable = agent_presentation_snapshot_from_rows(identity, execution, immutable_snapshot)?;
    let current = current_agent_identity_projection_from_rows(identity, current_snapshot)?;
    let mut author = immutable.to_turn_author_snapshot();

    // The nested AgentPresentationSnapshot remains the exact immutable
    // execution snapshot. Timeline labels use these top-level presentation
    // fields, which follow the current version of the stable AgentIdentity.
    author.display_name = current.display_name;
    author.nickname = current.nickname;
    author.avatar_revision = current.avatar_revision;
    Ok(author)
}

pub fn agent_identity_source_kind(source_kind: &str) -> Result<AgentIdentitySourceKind> {
    match source_kind {
        SOURCE_NATIVE_AGENT => Ok(AgentIdentitySourceKind::NativeAgent),
        SOURCE_CLI_RUNTIME_INSTANCE => Ok(AgentIdentitySourceKind::CliRuntimeInstance),
        SOURCE_EPHEMERAL => Ok(AgentIdentitySourceKind::Ephemeral),
        value => bail!("AgentIdentity has unsupported source kind `{value}`"),
    }
}

pub async fn revalidate_agent_presentation_for_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    projection: &AgentPresentationSnapshot,
) -> Result<()> {
    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent notification execution")?
        .context("Agent notification execution is missing")?;
    let identity = agent_identity::Entity::find_by_id(execution.agent_identity_id.clone())
        .one(db)
        .await
        .context("failed to load Agent notification identity")?
        .context("Agent notification identity is missing")?;
    let snapshot_id = execution
        .presentation_snapshot_id
        .as_deref()
        .context("Agent notification execution has no presentation snapshot")?;
    let snapshot = agent_presentation_snapshot::Entity::find_by_id(snapshot_id.to_owned())
        .one(db)
        .await
        .context("failed to load Agent notification presentation snapshot")?
        .context("Agent notification presentation snapshot is missing")?;
    let immutable = agent_presentation_snapshot_from_rows(&identity, &execution, &snapshot)?;
    if projection.agent_identity_id != immutable.agent_identity_id
        || projection.agent_execution_id != immutable.agent_execution_id
        || projection.identity_source_kind != immutable.identity_source_kind
        || projection.identity_source_revision != immutable.identity_source_revision
        || projection.display_name != immutable.display_name
        || projection.nickname != immutable.nickname
        || projection.avatar_revision != immutable.avatar_revision
        || projection.role_label != immutable.role_label
    {
        bail!("Agent notification author differs from its immutable execution presentation");
    }
    Ok(())
}

pub async fn ensure_agent_identity<C: ConnectionTrait>(
    db: &C,
    input: &AgentIdentityInput,
) -> Result<agent_identity::Model> {
    if input.source_revision < 1 {
        bail!("agent identity source revision must be positive");
    }
    agent_identity::Entity::insert(agent_identity::ActiveModel {
        id: Set(input.id.clone()),
        workspace_id: Set(input.workspace_id.clone()),
        source_kind: Set(input.source_kind.clone()),
        source_id: Set(input.source_id.clone()),
        source_revision: Set(input.source_revision),
        source_fingerprint: Set(input.source_fingerprint.clone()),
        status: Set("active".to_owned()),
        created_at: Set(input.now),
        updated_at: Set(input.now),
        retired_at: Set(None),
    })
    .on_conflict(
        OnConflict::columns([
            agent_identity::Column::WorkspaceId,
            agent_identity::Column::SourceKind,
            agent_identity::Column::SourceId,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent identity")?;
    let identity = load_agent_identity_by_source(
        db,
        &input.workspace_id,
        &input.source_kind,
        &input.source_id,
    )
    .await?
    .context("agent identity disappeared after idempotent insert")?;
    if identity.id != input.id
        || identity.source_revision != input.source_revision
        || identity.source_fingerprint != input.source_fingerprint
        || identity.status != "active"
        || identity.retired_at.is_some()
    {
        bail!("agent identity source was reused with different immutable facts");
    }
    Ok(identity)
}

pub async fn insert_presentation_snapshot<C: ConnectionTrait>(
    db: &C,
    input: &PresentationSnapshotInput,
) -> Result<agent_presentation_snapshot::Model> {
    use pioneer_entity::agent_presentation_snapshot;
    let existing = agent_presentation_snapshot::Entity::find()
        .filter(
            agent_presentation_snapshot::Column::AgentIdentityId
                .eq(input.agent_identity_id.clone()),
        )
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(input.source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(input.source_fingerprint.clone()),
        )
        .one(db)
        .await
        .context("failed to inspect presentation snapshot")?;
    if let Some(existing) = existing {
        if existing.id != input.id
            || existing.display_name != input.display_name
            || existing.nickname != input.nickname
            || existing.avatar_revision != input.avatar_revision
            || existing.role_label != input.role_label
        {
            bail!("presentation snapshot source was reused with different immutable facts");
        }
        return Ok(existing);
    }

    let insert_result =
        agent_presentation_snapshot::Entity::insert(agent_presentation_snapshot::ActiveModel {
            id: Set(input.id.clone()),
            agent_identity_id: Set(input.agent_identity_id.clone()),
            source_revision: Set(input.source_revision),
            source_fingerprint: Set(input.source_fingerprint.clone()),
            display_name: Set(input.display_name.clone()),
            nickname: Set(input.nickname.clone()),
            avatar_revision: Set(input.avatar_revision.clone()),
            role_label: Set(input.role_label.clone()),
            created_at: Set(input.now),
        })
        .on_conflict(
            OnConflict::columns([
                agent_presentation_snapshot::Column::AgentIdentityId,
                agent_presentation_snapshot::Column::SourceRevision,
                agent_presentation_snapshot::Column::SourceFingerprint,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec(db)
        .await;
    let row = agent_presentation_snapshot::Entity::find()
        .filter(
            agent_presentation_snapshot::Column::AgentIdentityId
                .eq(input.agent_identity_id.clone()),
        )
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(input.source_revision))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(input.source_fingerprint.clone()),
        )
        .one(db)
        .await
        .context("failed to reload presentation snapshot")?
        .context("presentation snapshot disappeared after idempotent insert")?;
    if let Err(error) = insert_result {
        if row.id == input.id
            && row.source_revision == input.source_revision
            && row.source_fingerprint == input.source_fingerprint
            && row.display_name == input.display_name
            && row.nickname == input.nickname
            && row.avatar_revision == input.avatar_revision
            && row.role_label == input.role_label
        {
            return Ok(row);
        }
        return Err(error).context("failed to persist immutable agent presentation snapshot");
    }
    Ok(row)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionAuthorProjection {
    pub author: pioneer_protocol::TurnAuthorSnapshot,
    pub presentation_snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnResponseInput {
    pub turn_id: String,
    pub execution_id: String,
    pub presentation_snapshot_id: String,
    pub now: DateTimeWithTimeZone,
}

pub async fn insert_agent_turn_response<C: ConnectionTrait>(
    db: &C,
    input: &AgentTurnResponseInput,
) -> Result<agent_turn_response_execution::Model> {
    if input.turn_id.trim().is_empty() {
        bail!("responding AgentExecution requires a Turn id");
    }
    let execution = agent_execution::Entity::find_by_id(input.execution_id.clone())
        .one(db)
        .await
        .context("failed to validate responding AgentExecution")?
        .context("responding AgentExecution does not exist")?;
    if execution.presentation_snapshot_id.as_deref()
        != Some(input.presentation_snapshot_id.as_str())
    {
        bail!("responding AgentExecution presentation snapshot is inconsistent");
    }
    agent_turn_response_execution::Entity::insert(agent_turn_response_execution::ActiveModel {
        turn_id: Set(input.turn_id.clone()),
        execution_id: Set(input.execution_id.clone()),
        presentation_snapshot_id: Set(input.presentation_snapshot_id.clone()),
        created_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::column(agent_turn_response_execution::Column::TurnId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist responding AgentExecution")?;
    let response = agent_turn_response_execution::Entity::find_by_id(input.turn_id.clone())
        .one(db)
        .await
        .context("failed to reload responding AgentExecution")?
        .context("responding AgentExecution disappeared after idempotent insert")?;
    if response.execution_id != input.execution_id
        || response.presentation_snapshot_id != input.presentation_snapshot_id
    {
        bail!("Agent Turn response was reused with different immutable facts");
    }
    Ok(response)
}

pub async fn load_agent_turn_responses_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<Vec<agent_turn_response_execution::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    if turn_ids.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent Turn response projection batch exceeds its bounded limit");
    }
    agent_turn_response_execution::Entity::find()
        .filter(agent_turn_response_execution::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to batch-load responding AgentExecutions")
}

/// Resolves exact AgentExecution actors with the current presentation of their
/// stable AgentIdentity. The nested execution snapshot remains immutable;
/// only the top-level timeline label and avatar fields are overlaid. If no
/// current presentation is available, the immutable execution snapshot is the
/// safe fallback, matching the existing Principal timeline behavior.
pub async fn load_current_agent_authors_for_executions<C: ConnectionTrait>(
    db: &C,
    execution_ids: &[String],
) -> Result<BTreeMap<String, AgentExecutionAuthorProjection>> {
    if execution_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    if execution_ids.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent author projection batch exceeds its bounded limit");
    }
    let runtime_kinds = crate::author_snapshot::cli_author_kinds(db, execution_ids).await?;
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::Id.is_in(execution_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to batch-load author AgentExecutions")?;
    let identity_ids = executions
        .iter()
        .map(|execution| execution.agent_identity_id.clone())
        .collect::<Vec<_>>();
    let snapshot_ids = executions
        .iter()
        .filter_map(|execution| execution.presentation_snapshot_id.clone())
        .collect::<Vec<_>>();
    let identities = agent_identity::Entity::find()
        .filter(agent_identity::Column::Id.is_in(identity_ids))
        .all(db)
        .await
        .context("failed to batch-load author AgentIdentities")?;
    let snapshots = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::Id.is_in(snapshot_ids))
        .all(db)
        .await
        .context("failed to batch-load author presentation snapshots")?;
    let current_snapshot_filter =
        identities
            .iter()
            .fold(Condition::any(), |condition, identity| {
                condition.add(
                    Condition::all()
                        .add(
                            agent_presentation_snapshot::Column::AgentIdentityId
                                .eq(identity.id.clone()),
                        )
                        .add(
                            agent_presentation_snapshot::Column::SourceRevision
                                .eq(identity.source_revision),
                        )
                        .add(
                            agent_presentation_snapshot::Column::SourceFingerprint
                                .eq(identity.source_fingerprint.clone()),
                        ),
                )
            });
    let current_snapshots = if identities.is_empty() {
        Vec::new()
    } else {
        agent_presentation_snapshot::Entity::find()
            .filter(current_snapshot_filter)
            .order_by_desc(agent_presentation_snapshot::Column::CreatedAt)
            .order_by_desc(agent_presentation_snapshot::Column::Id)
            .all(db)
            .await
            .context("failed to batch-load current Agent presentation snapshots")?
    };
    let identities_by_id = identities
        .iter()
        .map(|identity| (identity.id.as_str(), identity))
        .collect::<BTreeMap<_, _>>();
    let snapshots_by_id = snapshots
        .iter()
        .map(|snapshot| (snapshot.id.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();
    let mut current_snapshots_by_identity_id = BTreeMap::new();
    for snapshot in &current_snapshots {
        current_snapshots_by_identity_id
            .entry(snapshot.agent_identity_id.as_str())
            .or_insert(snapshot);
    }
    let mut result = BTreeMap::new();
    for execution in executions {
        let identity = identities_by_id
            .get(execution.agent_identity_id.as_str())
            .context("author AgentExecution identity is missing")?;
        let snapshot_id = execution
            .presentation_snapshot_id
            .as_deref()
            .context("author AgentExecution has no presentation snapshot")?;
        let snapshot = snapshots_by_id
            .get(snapshot_id)
            .context("author AgentExecution presentation snapshot is missing")?;
        let mut author = if let Some(current_snapshot) =
            current_snapshots_by_identity_id.get(identity.id.as_str())
        {
            current_agent_author_from_rows(identity, &execution, snapshot, current_snapshot)?
        } else {
            agent_presentation_snapshot_from_rows(identity, &execution, snapshot)?
                .to_turn_author_snapshot()
        };
        author.cli_runtime_kind = runtime_kinds.get(&execution.id).copied();
        result.insert(
            execution.id,
            AgentExecutionAuthorProjection {
                author,
                presentation_snapshot_id: snapshot_id.to_owned(),
            },
        );
    }
    Ok(result)
}

/// Persist a native identity source, its nickname claim and immutable
/// presentation in one transaction.
pub async fn commit_native_agent_config_mutation(
    db: &DatabaseTransaction,
    config: &NativeAgentConfigInput,
    identity: &AgentIdentityInput,
    presentation: &PresentationSnapshotInput,
) -> Result<(
    native_agent_config::Model,
    agent_identity::Model,
    agent_presentation_snapshot::Model,
)> {
    if config.workspace_id != identity.workspace_id
        || identity.source_kind != SOURCE_NATIVE_AGENT
        || identity.source_id != config.id
    {
        bail!("native config and identity workspace differ");
    }
    if identity.id != presentation.agent_identity_id
        || config.config_revision != identity.source_revision
        || identity.source_revision != presentation.source_revision
        || identity.source_fingerprint != presentation.source_fingerprint
        || config.display_name != presentation.display_name
        || config.nickname != presentation.nickname
        || config.avatar_revision != presentation.avatar_revision
        || config.config_revision < 1
    {
        bail!("native config presentation is bound to a different identity");
    }
    let existing_config = load_native_agent_config(db, config.id.as_str()).await?;
    let config_row = if let Some(existing) = existing_config {
        if existing.workspace_id != config.workspace_id || existing.system_key != config.system_key
        {
            bail!("native agent config source ownership is immutable");
        }
        let source_changed = existing.display_name != config.display_name
            || existing.nickname != config.nickname
            || existing.enabled != config.enabled
            || existing.avatar_revision != config.avatar_revision;
        if existing.system_key.as_deref() == Some("pioneer") && source_changed {
            bail!("reserved Pioneer native config cannot be modified");
        }
        if source_changed {
            let expected_revision = existing
                .config_revision
                .checked_add(1)
                .context("native agent config revision exhausted")?;
            if config.config_revision != expected_revision {
                bail!("native agent config changed concurrently");
            }
            let updated = native_agent_config::Entity::update_many()
                .col_expr(
                    native_agent_config::Column::DisplayName,
                    sea_orm::sea_query::Expr::value(config.display_name.clone()),
                )
                .col_expr(
                    native_agent_config::Column::Nickname,
                    sea_orm::sea_query::Expr::value(config.nickname.clone()),
                )
                .col_expr(
                    native_agent_config::Column::Enabled,
                    sea_orm::sea_query::Expr::value(config.enabled),
                )
                .col_expr(
                    native_agent_config::Column::AvatarRevision,
                    sea_orm::sea_query::Expr::value(config.avatar_revision.clone()),
                )
                .col_expr(
                    native_agent_config::Column::ConfigRevision,
                    sea_orm::sea_query::Expr::value(config.config_revision),
                )
                .col_expr(
                    native_agent_config::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(config.now.clone()),
                )
                .filter(native_agent_config::Column::Id.eq(config.id.clone()))
                .filter(native_agent_config::Column::ConfigRevision.eq(existing.config_revision))
                .exec(db)
                .await
                .context("failed to update native agent config")?;
            if updated.rows_affected != 1 {
                bail!("native agent config changed concurrently");
            }
            load_native_agent_config(db, config.id.as_str())
                .await?
                .context("native agent config disappeared after update")?
        } else {
            if existing.config_revision != config.config_revision {
                bail!("idempotent native agent config mutation has a different revision");
            }
            existing
        }
    } else {
        if config.config_revision != 1 {
            bail!("new native agent config must start at revision one");
        }
        ensure_native_agent_config(db, config).await?
    };

    let existing_identity = load_agent_identity_by_source(
        db,
        config.workspace_id.as_str(),
        SOURCE_NATIVE_AGENT,
        config.id.as_str(),
    )
    .await?;
    let identity_row = if let Some(existing) = existing_identity {
        if existing.id != identity.id
            || existing.workspace_id != identity.workspace_id
            || existing.status != "active"
            || existing.retired_at.is_some()
        {
            bail!("native config maps to a different or retired identity");
        }
        if existing.source_revision == identity.source_revision
            && existing.source_fingerprint == identity.source_fingerprint
        {
            existing
        } else {
            if identity.source_revision
                != existing
                    .source_revision
                    .checked_add(1)
                    .context("native identity source revision exhausted")?
                || config_row.config_revision != identity.source_revision
            {
                bail!("native identity source changed concurrently");
            }
            let updated = agent_identity::Entity::update_many()
                .col_expr(
                    agent_identity::Column::SourceRevision,
                    sea_orm::sea_query::Expr::value(identity.source_revision),
                )
                .col_expr(
                    agent_identity::Column::SourceFingerprint,
                    sea_orm::sea_query::Expr::value(identity.source_fingerprint.clone()),
                )
                .col_expr(
                    agent_identity::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(config.now.clone()),
                )
                .filter(agent_identity::Column::Id.eq(existing.id.clone()))
                .filter(agent_identity::Column::SourceRevision.eq(existing.source_revision))
                .filter(agent_identity::Column::Status.eq("active"))
                .exec(db)
                .await
                .context("failed to update native identity source revision")?;
            if updated.rows_affected != 1 {
                bail!("native identity source changed concurrently");
            }
            load_agent_identity(db, existing.id.as_str())
                .await?
                .context("native identity disappeared after update")?
        }
    } else {
        if identity.source_revision != 1 || config_row.config_revision != 1 {
            bail!("new native identity source must start at revision one");
        }
        ensure_agent_identity(db, identity).await?
    };
    claim_actor_nickname(
        db,
        config.workspace_id.as_str(),
        config.nickname.as_str(),
        NICKNAME_OWNER_AGENT,
        identity_row.id.as_str(),
        config.now.clone(),
    )
    .await?;
    let presentation_row = insert_presentation_snapshot(db, presentation).await?;
    Ok((config_row, identity_row, presentation_row))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_config_identity_and_presentation_share_one_commit() -> Result<()> {
        use sea_orm::TransactionTrait;

        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared(
                "INSERT INTO workspace(id, name) VALUES ('agent-ws', 'Agent workspace')",
            )
            .await?;
            let now = crate::time::unix_to_datetime(1_700_000_000);
            let config = NativeAgentConfigInput {
                id: "N00000000000000000001".to_owned(),
                workspace_id: "agent-ws".to_owned(),
                system_key: None,
                display_name: "Native agent".to_owned(),
                nickname: "native".to_owned(),
                enabled: true,
                avatar_revision: None,
                config_revision: 1,
                now,
            };
            let identity = AgentIdentityInput {
                id: "I00000000000000000001".to_owned(),
                workspace_id: config.workspace_id.clone(),
                source_kind: SOURCE_NATIVE_AGENT.to_owned(),
                source_id: config.id.clone(),
                source_revision: 1,
                source_fingerprint: "fingerprint".to_owned(),
                now,
            };
            let presentation = PresentationSnapshotInput {
                id: "S00000000000000000001".to_owned(),
                agent_identity_id: identity.id.clone(),
                source_revision: 1,
                source_fingerprint: identity.source_fingerprint.clone(),
                display_name: config.display_name.clone(),
                nickname: config.nickname.clone(),
                avatar_revision: None,
                role_label: None,
                now,
            };
            let observer = fixture.connect().await?;
            for commit in [false, true] {
                let tx = db.begin().await?;
                let original =
                    commit_native_agent_config_mutation(&tx, &config, &identity, &presentation)
                        .await?;
                assert_eq!(
                    commit_native_agent_config_mutation(&tx, &config, &identity, &presentation,)
                        .await?,
                    original
                );
                assert!(
                    load_native_agent_config(&observer, &config.id)
                        .await?
                        .is_none()
                );
                assert!(
                    load_agent_identity(&observer, &identity.id)
                        .await?
                        .is_none()
                );
                if commit {
                    tx.commit().await?;
                    assert_eq!(
                        load_native_agent_config(&observer, &config.id).await?,
                        Some(original.0)
                    );
                    assert_eq!(
                        load_agent_identity(&observer, &identity.id).await?,
                        Some(original.1)
                    );
                    assert_eq!(
                        load_agent_presentation_snapshot(&observer, &presentation.id).await?,
                        Some(original.2)
                    );
                } else {
                    tx.rollback().await?;
                    assert!(
                        load_native_agent_config(&observer, &config.id)
                            .await?
                            .is_none()
                    );
                    assert!(
                        load_agent_identity(&observer, &identity.id)
                            .await?
                            .is_none()
                    );
                }
            }
            observer.close().await?;
            Ok(())
        })
        .await
    }

    #[test]
    fn timeline_agent_author_uses_current_identity_presentation_without_rewriting_execution() {
        let now = chrono::Utc::now().fixed_offset();
        let identity = agent_identity::Model {
            id: "A0000000000000000000A".to_owned(),
            workspace_id: "workspace".to_owned(),
            source_kind: SOURCE_CLI_RUNTIME_INSTANCE.to_owned(),
            source_id: "codex".to_owned(),
            source_revision: 2,
            source_fingerprint: "current-fingerprint".to_owned(),
            status: "active".to_owned(),
            created_at: now,
            updated_at: now,
            retired_at: None,
        };
        let execution = agent_execution::Model {
            id: "E0000000000000000000A".to_owned(),
            workspace_id: "workspace".to_owned(),
            agent_identity_id: identity.id.clone(),
            identity_source_revision: 1,
            identity_source_fingerprint: "historical-fingerprint".to_owned(),
            parent_execution_id: None,
            parent_task_id: None,
            parent_thread_id: None,
            home_root_thread_id: "thread".to_owned(),
            work_graph_root_execution_id: "E0000000000000000000A".to_owned(),
            requested_identity_selection_json: "{}".to_owned(),
            requested_profile_selection_json: "{}".to_owned(),
            resolved_profile_id: None,
            resolved_profile_fingerprint: None,
            presentation_snapshot_id: Some("snapshot-historical".to_owned()),
            authorization_context_fingerprint: "authorization".to_owned(),
            execution_generation: 1,
            status: "completed".to_owned(),
            created_at: now,
            updated_at: now,
            finished_at: Some(now),
        };
        let immutable_snapshot = agent_presentation_snapshot::Model {
            id: "snapshot-historical".to_owned(),
            agent_identity_id: identity.id.clone(),
            source_revision: 1,
            source_fingerprint: "historical-fingerprint".to_owned(),
            display_name: "Codex CLI".to_owned(),
            nickname: "codex".to_owned(),
            avatar_revision: Some("historical-avatar".to_owned()),
            role_label: Some("codex".to_owned()),
            created_at: now,
        };
        let current_snapshot = agent_presentation_snapshot::Model {
            id: "snapshot-current".to_owned(),
            agent_identity_id: identity.id.clone(),
            source_revision: 2,
            source_fingerprint: "current-fingerprint".to_owned(),
            display_name: "Renamed Codex".to_owned(),
            nickname: "renamed-codex".to_owned(),
            avatar_revision: Some("current-avatar".to_owned()),
            role_label: Some("codex".to_owned()),
            created_at: now,
        };

        let author = current_agent_author_from_rows(
            &identity,
            &execution,
            &immutable_snapshot,
            &current_snapshot,
        )
        .unwrap();

        assert_eq!(author.display_name, "Renamed Codex");
        assert_eq!(author.nickname, "renamed-codex");
        assert_eq!(author.avatar_revision.as_deref(), Some("current-avatar"));
        assert!(matches!(
            author.actor,
            pioneer_protocol::PersistedActorRef::AgentExecution(ref execution_id)
                if execution_id.as_str() == execution.id
        ));
        let immutable = author.agent.expect("immutable execution presentation");
        assert_eq!(immutable.identity_source_revision, 1);
        assert_eq!(immutable.display_name, "Codex CLI");
        assert_eq!(immutable.nickname, "codex");
        assert_eq!(
            immutable.avatar_revision.as_deref(),
            Some("historical-avatar")
        );
    }
}
