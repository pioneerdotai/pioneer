use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use pioneer_entity::{agent_skill, agent_skill_version};
use pioneer_protocol::{SKILL_ID_LEN, SkillId};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};

use crate::{AgentSkillVersionRecord, AgentSkillVersionSnapshotRecord};

const SOURCE_TURN_IDS_JSON_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub(super) struct NewAgentSkill {
    pub skill_id: SkillId,
    pub workspace_id: String,
    pub slug: String,
}

#[derive(Debug, Clone)]
pub(super) struct NewAgentSkillVersion {
    pub id: String,
    pub skill_id: SkillId,
    pub version_number: i64,
    pub source_run_id: Option<String>,
    pub parent_version_id: Option<String>,
    pub candidate_key: String,
    pub display_name: String,
    pub skill_markdown: String,
    pub instruction_body: String,
    pub when_to_use: String,
    pub when_not_to_use: String,
    pub fingerprint: String,
    pub source_turn_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedAgentSkillVersion {
    pub id: String,
    pub skill_id: SkillId,
    pub version_number: i64,
    pub source_run_id: Option<String>,
    pub parent_version_id: Option<String>,
    pub candidate_key: String,
    pub display_name: String,
    pub skill_markdown: String,
    pub instruction_body: String,
    pub when_to_use: String,
    pub when_not_to_use: String,
    pub fingerprint: String,
    source_turn_ids_json: String,
}

impl PreparedAgentSkillVersion {
    pub(super) fn source_turn_ids_json(&self) -> &str {
        self.source_turn_ids_json.as_str()
    }
}

pub(super) fn prepare_agent_skill_version(
    input: NewAgentSkillVersion,
) -> Result<PreparedAgentSkillVersion> {
    validate_version(&input)?;
    let source_turn_ids_json = serde_json::to_string(&input.source_turn_ids)
        .context("failed to encode Agent skill source turn IDs")?;
    if source_turn_ids_json.len() > SOURCE_TURN_IDS_JSON_MAX_BYTES {
        bail!(
            "Agent skill source_turn_ids_json exceeds its {}-byte persistence limit",
            SOURCE_TURN_IDS_JSON_MAX_BYTES
        );
    }
    Ok(PreparedAgentSkillVersion {
        id: input.id,
        skill_id: input.skill_id,
        version_number: input.version_number,
        source_run_id: input.source_run_id,
        parent_version_id: input.parent_version_id,
        candidate_key: input.candidate_key,
        display_name: input.display_name,
        skill_markdown: input.skill_markdown,
        instruction_body: input.instruction_body,
        when_to_use: input.when_to_use,
        when_not_to_use: input.when_not_to_use,
        fingerprint: input.fingerprint,
        source_turn_ids_json,
    })
}

#[derive(Debug, Clone)]
pub(super) struct CreateAgentSkillMutation {
    pub skill: NewAgentSkill,
    pub version: PreparedAgentSkillVersion,
}

#[derive(Debug, Clone)]
pub(super) struct UpdateAgentSkillMutation {
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub expected_active_version_id: String,
    pub expected_slug: String,
    pub version: PreparedAgentSkillVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum UpdateAgentSkillMutationResult {
    Applied {
        previous_version_id: String,
        resulting_version_id: String,
    },
    CurrentFingerprintNoChange,
    HistoricalFingerprintNoChange {
        existing_version_id: String,
    },
    ExactParentFingerprintRequiresRollback {
        parent_version_id: String,
    },
    StaleActive,
    Rejected(&'static str),
}

#[derive(Debug, Clone)]
pub(super) struct RollbackAgentSkillMutation {
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub expected_active_version_id: String,
    pub target_parent_version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RollbackAgentSkillMutationResult {
    Applied {
        previous_version_id: String,
        resulting_version_id: String,
    },
    StaleActive,
    Rejected(&'static str),
}

pub(super) async fn apply_create_in_caller_transaction(
    db: &DatabaseTransaction,
    input: CreateAgentSkillMutation,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    if input.version.skill_id != input.skill.skill_id
        || input.version.version_number != 1
        || input.version.parent_version_id.is_some()
    {
        bail!("Agent skill create mutation has inconsistent identity or lineage");
    }
    insert_logical_skill(db, input.skill.clone(), now).await?;
    insert_immutable_version(db, input.version.clone(), now).await?;
    if !set_active_version_if(
        db,
        input.skill.workspace_id.as_str(),
        &input.skill.skill_id,
        None,
        input.version.id.as_str(),
        now,
    )
    .await?
    {
        bail!("new Agent skill active pointer changed inside caller transaction");
    }
    Ok(())
}

pub(super) async fn apply_update_in_caller_transaction(
    db: &DatabaseTransaction,
    input: UpdateAgentSkillMutation,
    now: DateTimeWithTimeZone,
) -> Result<UpdateAgentSkillMutationResult> {
    if input.workspace_id.trim().is_empty()
        || input.expected_active_version_id.trim().is_empty()
        || input.expected_slug.trim().is_empty()
        || input.version.skill_id != input.skill_id
        || input.version.parent_version_id.as_deref()
            != Some(input.expected_active_version_id.as_str())
    {
        return Ok(UpdateAgentSkillMutationResult::Rejected(
            "update_identity_or_lineage_invalid",
        ));
    }
    let Some(skill) = agent_skill::Entity::find_by_id(input.skill_id.as_str().to_owned())
        .filter(agent_skill::Column::WorkspaceId.eq(input.workspace_id.clone()))
        .one(db)
        .await
        .context("failed to load exact Agent skill update target")?
    else {
        return Ok(UpdateAgentSkillMutationResult::Rejected(
            "update_target_not_found",
        ));
    };
    if skill.slug != input.expected_slug {
        return Ok(UpdateAgentSkillMutationResult::Rejected(
            "update_slug_changed",
        ));
    }
    if skill.active_version_id.as_deref() != Some(input.expected_active_version_id.as_str()) {
        return Ok(UpdateAgentSkillMutationResult::StaleActive);
    }
    let Some(active) =
        agent_skill_version::Entity::find_by_id(input.expected_active_version_id.clone())
            .filter(agent_skill_version::Column::SkillId.eq(input.skill_id.as_str().to_owned()))
            .one(db)
            .await
            .context("failed to load exact active Agent skill version for update")?
    else {
        return Ok(UpdateAgentSkillMutationResult::StaleActive);
    };
    if active.fingerprint == input.version.fingerprint {
        return Ok(UpdateAgentSkillMutationResult::CurrentFingerprintNoChange);
    }
    if let Some(existing) = agent_skill_version::Entity::find()
        .filter(agent_skill_version::Column::SkillId.eq(input.skill_id.as_str().to_owned()))
        .filter(agent_skill_version::Column::Fingerprint.eq(input.version.fingerprint.clone()))
        .one(db)
        .await
        .context("failed to apply Agent skill update fingerprint policy")?
    {
        if active.parent_version_id.as_deref() == Some(existing.id.as_str()) {
            return Ok(
                UpdateAgentSkillMutationResult::ExactParentFingerprintRequiresRollback {
                    parent_version_id: existing.id,
                },
            );
        }
        return Ok(
            UpdateAgentSkillMutationResult::HistoricalFingerprintNoChange {
                existing_version_id: existing.id,
            },
        );
    }
    let expected_version_number = next_version_number_for_skill(db, &input.skill_id).await?;
    if input.version.version_number != expected_version_number {
        return Ok(UpdateAgentSkillMutationResult::StaleActive);
    }
    insert_immutable_version(db, input.version.clone(), now).await?;
    if !set_active_version_if(
        db,
        input.workspace_id.as_str(),
        &input.skill_id,
        Some(input.expected_active_version_id.as_str()),
        input.version.id.as_str(),
        now,
    )
    .await?
    {
        bail!(
            "Agent skill active pointer changed after inserting update version; \
             caller must roll back the terminal transaction"
        );
    }
    Ok(UpdateAgentSkillMutationResult::Applied {
        previous_version_id: input.expected_active_version_id,
        resulting_version_id: input.version.id,
    })
}

pub(super) async fn apply_rollback_in_caller_transaction(
    db: &DatabaseTransaction,
    input: RollbackAgentSkillMutation,
    now: DateTimeWithTimeZone,
) -> Result<RollbackAgentSkillMutationResult> {
    if input.workspace_id.trim().is_empty()
        || input.expected_active_version_id.trim().is_empty()
        || input.target_parent_version_id.trim().is_empty()
        || input.expected_active_version_id == input.target_parent_version_id
    {
        return Ok(RollbackAgentSkillMutationResult::Rejected(
            "rollback_identity_invalid",
        ));
    }
    let Some(skill) = agent_skill::Entity::find_by_id(input.skill_id.as_str().to_owned())
        .filter(agent_skill::Column::WorkspaceId.eq(input.workspace_id.clone()))
        .one(db)
        .await
        .context("failed to load exact Agent skill rollback target")?
    else {
        return Ok(RollbackAgentSkillMutationResult::Rejected(
            "rollback_target_not_found",
        ));
    };
    if skill.active_version_id.as_deref() != Some(input.expected_active_version_id.as_str()) {
        return Ok(RollbackAgentSkillMutationResult::StaleActive);
    }
    let Some(active) =
        agent_skill_version::Entity::find_by_id(input.expected_active_version_id.clone())
            .filter(agent_skill_version::Column::SkillId.eq(input.skill_id.as_str().to_owned()))
            .one(db)
            .await
            .context("failed to load exact active Agent skill version for rollback")?
    else {
        return Ok(RollbackAgentSkillMutationResult::StaleActive);
    };
    if active.parent_version_id.as_deref() != Some(input.target_parent_version_id.as_str()) {
        return Ok(RollbackAgentSkillMutationResult::Rejected(
            "rollback_target_not_exact_parent",
        ));
    }
    if agent_skill_version::Entity::find_by_id(input.target_parent_version_id.clone())
        .filter(agent_skill_version::Column::SkillId.eq(input.skill_id.as_str().to_owned()))
        .one(db)
        .await
        .context("failed to load exact parent Agent skill version for rollback")?
        .is_none()
    {
        return Ok(RollbackAgentSkillMutationResult::Rejected(
            "rollback_parent_not_owned",
        ));
    }
    if !set_active_version_if(
        db,
        input.workspace_id.as_str(),
        &input.skill_id,
        Some(input.expected_active_version_id.as_str()),
        input.target_parent_version_id.as_str(),
        now,
    )
    .await?
    {
        bail!(
            "Agent skill active pointer changed during rollback; \
             caller must roll back the terminal transaction"
        );
    }
    Ok(RollbackAgentSkillMutationResult::Applied {
        previous_version_id: input.expected_active_version_id,
        resulting_version_id: input.target_parent_version_id,
    })
}

pub async fn list_active_versions<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<AgentSkillVersionSnapshotRecord>> {
    let skills = agent_skill::Entity::find()
        .filter(agent_skill::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_skill::Column::ActiveVersionId.is_not_null())
        .order_by_asc(agent_skill::Column::Slug)
        .order_by_asc(agent_skill::Column::Id)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load active Agent skills for workspace `{workspace_id}`")
        })?;
    if skills.is_empty() {
        return Ok(Vec::new());
    }

    let version_ids = skills
        .iter()
        .filter_map(|skill| skill.active_version_id.clone())
        .collect::<Vec<_>>();
    let versions = agent_skill_version::Entity::find()
        .filter(agent_skill_version::Column::Id.is_in(version_ids))
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load active Agent skill versions for workspace `{workspace_id}`")
        })?
        .into_iter()
        .map(|version| (version.id.clone(), version))
        .collect::<HashMap<_, _>>();

    skills
        .into_iter()
        .map(|skill| {
            let version_id = skill.active_version_id.as_deref().with_context(|| {
                format!("Agent skill `{}` has no committed active version", skill.id)
            })?;
            let version = versions.get(version_id).cloned().with_context(|| {
                format!(
                    "Agent skill `{}` points to missing active version `{version_id}`",
                    skill.id
                )
            })?;
            active_record_from_models(skill, version)
        })
        .collect()
}

pub async fn find_exact_version<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    version_id: &str,
) -> Result<Option<AgentSkillVersionSnapshotRecord>> {
    let Some(version) = agent_skill_version::Entity::find_by_id(version_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load Agent skill version `{version_id}`"))?
    else {
        return Ok(None);
    };
    let Some(skill) = agent_skill::Entity::find_by_id(version.skill_id.clone())
        .filter(agent_skill::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify workspace `{workspace_id}` for Agent skill version \
                 `{version_id}`"
            )
        })?
    else {
        return Ok(None);
    };

    active_record_from_models(skill, version).map(Some)
}

pub async fn find_next_version_number<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    skill_id: &SkillId,
) -> Result<Option<i64>> {
    if agent_skill::Entity::find_by_id(skill_id.as_str().to_owned())
        .filter(agent_skill::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify workspace `{workspace_id}` for Agent skill `{}`",
                skill_id.as_str()
            )
        })?
        .is_none()
    {
        return Ok(None);
    }
    next_version_number_for_skill(db, skill_id).await.map(Some)
}

pub async fn list_workspace_version_fingerprints<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
) -> Result<Vec<String>> {
    let skill_ids = agent_skill::Entity::find()
        .select_only()
        .column(agent_skill::Column::Id)
        .filter(agent_skill::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .into_tuple::<String>()
        .all(db)
        .await
        .with_context(|| {
            format!("failed to load Agent skill IDs for workspace `{workspace_id}`")
        })?;
    if skill_ids.is_empty() {
        return Ok(Vec::new());
    }
    agent_skill_version::Entity::find()
        .select_only()
        .column(agent_skill_version::Column::Fingerprint)
        .filter(agent_skill_version::Column::SkillId.is_in(skill_ids))
        .order_by_asc(agent_skill_version::Column::SkillId)
        .order_by_asc(agent_skill_version::Column::VersionNumber)
        .into_tuple::<String>()
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to load Agent skill version fingerprints for workspace `{workspace_id}`"
            )
        })
}

async fn next_version_number_for_skill<C: ConnectionTrait>(
    db: &C,
    skill_id: &SkillId,
) -> Result<i64> {
    let latest = agent_skill_version::Entity::find()
        .filter(agent_skill_version::Column::SkillId.eq(skill_id.as_str().to_owned()))
        .order_by_desc(agent_skill_version::Column::VersionNumber)
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load latest immutable version for Agent skill `{}`",
                skill_id.as_str()
            )
        })?
        .with_context(|| {
            format!(
                "Agent skill `{}` has no immutable version",
                skill_id.as_str()
            )
        })?;
    latest.version_number.checked_add(1).with_context(|| {
        format!(
            "Agent skill `{}` version number overflowed",
            skill_id.as_str()
        )
    })
}

async fn insert_logical_skill<C: ConnectionTrait>(
    db: &C,
    input: NewAgentSkill,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    validate_identity(input.workspace_id.as_str(), input.slug.as_str())?;
    agent_skill::ActiveModel {
        id: Set(input.skill_id.as_str().to_owned()),
        workspace_id: Set(input.workspace_id),
        slug: Set(input.slug),
        active_version_id: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert logical Agent skill `{}`",
            input.skill_id.as_str()
        )
    })?;
    Ok(())
}

async fn insert_immutable_version<C: ConnectionTrait>(
    db: &C,
    input: PreparedAgentSkillVersion,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    agent_skill_version::ActiveModel {
        id: Set(input.id.clone()),
        skill_id: Set(input.skill_id.as_str().to_owned()),
        version_number: Set(input.version_number),
        source_run_id: Set(input.source_run_id),
        parent_version_id: Set(input.parent_version_id),
        candidate_key: Set(input.candidate_key),
        display_name: Set(input.display_name),
        skill_markdown: Set(input.skill_markdown),
        instruction_body: Set(input.instruction_body),
        when_to_use: Set(input.when_to_use),
        when_not_to_use: Set(input.when_not_to_use),
        fingerprint: Set(input.fingerprint),
        source_turn_ids_json: Set(input.source_turn_ids_json),
        created_at: Set(now),
    }
    .insert(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert immutable Agent skill version `{}`",
            input.id
        )
    })?;
    Ok(())
}

async fn set_active_version_if<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    skill_id: &SkillId,
    expected_active_version_id: Option<&str>,
    new_active_version_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let target = agent_skill_version::Entity::find_by_id(new_active_version_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to load Agent skill version `{new_active_version_id}` for activation")
        })?
        .with_context(|| format!("Agent skill version `{new_active_version_id}` does not exist"))?;
    if target.skill_id != skill_id.as_str() {
        bail!(
            "Agent skill version `{new_active_version_id}` belongs to `{}`, not `{}`",
            target.skill_id,
            skill_id.as_str()
        );
    }

    let mut update = agent_skill::Entity::update_many()
        .col_expr(
            agent_skill::Column::ActiveVersionId,
            Expr::value(Some(new_active_version_id.to_owned())),
        )
        .col_expr(agent_skill::Column::UpdatedAt, Expr::value(updated_at))
        .filter(agent_skill::Column::Id.eq(skill_id.as_str().to_owned()))
        .filter(agent_skill::Column::WorkspaceId.eq(workspace_id.to_owned()));
    update = match expected_active_version_id {
        Some(expected) => {
            update.filter(agent_skill::Column::ActiveVersionId.eq(expected.to_owned()))
        }
        None => update.filter(agent_skill::Column::ActiveVersionId.is_null()),
    };
    Ok(update
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to set active version for Agent skill `{}` in workspace `{workspace_id}`",
                skill_id.as_str()
            )
        })?
        .rows_affected
        == 1)
}

fn validate_identity(workspace_id: &str, slug: &str) -> Result<()> {
    if workspace_id.trim().is_empty() {
        bail!("Agent skill workspace_id must not be empty");
    }
    if slug.trim().is_empty() || slug != slug.trim() {
        bail!("Agent skill slug must be non-empty and normalized");
    }
    Ok(())
}

fn validate_version(input: &NewAgentSkillVersion) -> Result<()> {
    if input.id.len() != SKILL_ID_LEN
        || !input
            .id
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
    {
        bail!("Agent skill version id must be a {SKILL_ID_LEN}-character alphanumeric identifier");
    }
    if input.version_number < 1 {
        bail!("Agent skill version_number must be positive");
    }
    for (name, value) in [
        ("candidate_key", input.candidate_key.as_str()),
        ("display_name", input.display_name.as_str()),
        ("skill_markdown", input.skill_markdown.as_str()),
        ("instruction_body", input.instruction_body.as_str()),
        ("when_to_use", input.when_to_use.as_str()),
        ("when_not_to_use", input.when_not_to_use.as_str()),
        ("fingerprint", input.fingerprint.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("Agent skill version {name} must not be empty");
        }
    }
    validate_source_turn_ids(&input.source_turn_ids)
}

fn validate_source_turn_ids(source_turn_ids: &[String]) -> Result<()> {
    if source_turn_ids
        .iter()
        .any(|source_id| source_id.trim().is_empty() || source_id != source_id.trim())
    {
        bail!("Agent skill source turn IDs must be non-empty and normalized");
    }
    let mut sorted = source_turn_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.len() != source_turn_ids.len() {
        bail!("Agent skill source turn IDs must be distinct");
    }
    Ok(())
}

fn active_record_from_models(
    skill: agent_skill::Model,
    version: agent_skill_version::Model,
) -> Result<AgentSkillVersionSnapshotRecord> {
    if version.skill_id != skill.id {
        bail!(
            "Agent skill `{}` points to version `{}` owned by `{}`",
            skill.id,
            version.id,
            version.skill_id
        );
    }
    let skill_id = SkillId::new(skill.id.clone()).with_context(|| {
        format!(
            "Agent skill primary key `{}` is not a valid SkillId",
            skill.id
        )
    })?;
    if version.source_turn_ids_json.len() > SOURCE_TURN_IDS_JSON_MAX_BYTES {
        bail!(
            "Agent skill version `{}` source_turn_ids_json exceeds its persistence limit",
            version.id
        );
    }
    let source_turn_ids = serde_json::from_str::<Vec<String>>(
        version.source_turn_ids_json.as_str(),
    )
    .with_context(|| {
        format!(
            "Agent skill version `{}` contains invalid source_turn_ids_json",
            version.id
        )
    })?;
    validate_source_turn_ids(&source_turn_ids).with_context(|| {
        format!(
            "Agent skill version `{}` contains invalid source turn identities",
            version.id
        )
    })?;

    Ok(AgentSkillVersionSnapshotRecord {
        skill_id,
        workspace_id: skill.workspace_id,
        slug: skill.slug,
        version: AgentSkillVersionRecord {
            id: version.id,
            version_number: version.version_number,
            source_run_id: version.source_run_id,
            parent_version_id: version.parent_version_id,
            candidate_key: version.candidate_key,
            display_name: version.display_name,
            skill_markdown: version.skill_markdown,
            instruction_body: version.instruction_body,
            when_to_use: version.when_to_use,
            when_not_to_use: version.when_not_to_use,
            fingerprint: version.fingerprint,
            source_turn_ids,
            created_at_unix: version.created_at.timestamp(),
        },
    })
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, PaginatorTrait, TransactionTrait,
    };

    use super::*;
    use crate::util::unix_to_datetime;

    const NOW: i64 = 1_900_200_000;
    const SKILL_A: &str = "AAAAAAAAAAAAAAAAAAAAA";
    const SKILL_B: &str = "BBBBBBBBBBBBBBBBBBBBB";
    const VERSION_A1: &str = "111111111111111111111";
    const VERSION_A2: &str = "222222222222222222222";
    const VERSION_B1: &str = "333333333333333333333";
    const VERSION_A3: &str = "444444444444444444444";
    const VERSION_A4: &str = "555555555555555555555";
    const RUN_ID: &str = "RRRRRRRRRRRRRRRRRRRRR";

    async fn database() -> DatabaseConnection {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite must open");
        Migrator::up(&database, None)
            .await
            .expect("migrations must apply");
        database
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
                 ('ws_skill_a', 'Skill A', 1, 1), \
                 ('ws_skill_b', 'Skill B', 1, 0)",
            )
            .await
            .expect("workspace fixtures must insert");
        database
    }

    fn skill(value: &str) -> SkillId {
        SkillId::new(value).expect("fixture must be a valid SkillId")
    }

    fn version(
        id: &str,
        skill_id: &str,
        version_number: i64,
        source_run_id: Option<&str>,
        candidate_key: &str,
        fingerprint: &str,
    ) -> PreparedAgentSkillVersion {
        prepare_agent_skill_version(NewAgentSkillVersion {
            id: id.to_owned(),
            skill_id: skill(skill_id),
            version_number,
            source_run_id: source_run_id.map(str::to_owned),
            parent_version_id: (version_number > 1).then(|| VERSION_A1.to_owned()),
            candidate_key: candidate_key.to_owned(),
            display_name: format!("Skill version {version_number}"),
            skill_markdown: format!(
                "---\nname: example\n---\n\nInstruction version {version_number}."
            ),
            instruction_body: format!("Instruction version {version_number}."),
            when_to_use: "Use for the tested procedure.".to_owned(),
            when_not_to_use: "Do not use outside the tested procedure.".to_owned(),
            fingerprint: fingerprint.to_owned(),
            source_turn_ids: vec!["turn-source-one".to_owned(), "turn-source-two".to_owned()],
        })
        .expect("fixture Agent skill version must prepare")
    }

    #[tokio::test]
    async fn active_and_exact_reads_are_workspace_scoped_and_version_exact() {
        let database = database().await;
        let transaction = database.begin().await.expect("transaction must begin");
        insert_logical_skill(
            &transaction,
            NewAgentSkill {
                skill_id: skill(SKILL_A),
                workspace_id: "ws_skill_a".to_owned(),
                slug: "stable-procedure".to_owned(),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("logical skill A must insert");
        insert_immutable_version(
            &transaction,
            version(
                VERSION_A1,
                SKILL_A,
                1,
                None,
                "candidate-a1",
                "fingerprint-a1",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect("version A1 must insert");
        assert!(
            set_active_version_if(
                &transaction,
                "ws_skill_a",
                &skill(SKILL_A),
                None,
                VERSION_A1,
                unix_to_datetime(NOW),
            )
            .await
            .expect("active pointer A1 must update")
        );
        insert_immutable_version(
            &transaction,
            version(
                VERSION_A2,
                SKILL_A,
                2,
                None,
                "candidate-a2",
                "fingerprint-a2",
            ),
            unix_to_datetime(NOW + 1),
        )
        .await
        .expect("historical version A2 must insert without changing active pointer");

        insert_logical_skill(
            &transaction,
            NewAgentSkill {
                skill_id: skill(SKILL_B),
                workspace_id: "ws_skill_b".to_owned(),
                slug: "other-workspace".to_owned(),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("logical skill B must insert");
        insert_immutable_version(
            &transaction,
            version(
                VERSION_B1,
                SKILL_B,
                1,
                None,
                "candidate-b1",
                "fingerprint-b1",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect("version B1 must insert");
        assert!(
            set_active_version_if(
                &transaction,
                "ws_skill_b",
                &skill(SKILL_B),
                None,
                VERSION_B1,
                unix_to_datetime(NOW),
            )
            .await
            .expect("active pointer B1 must update")
        );
        transaction.commit().await.expect("transaction must commit");

        let active_a = list_active_versions(&database, "ws_skill_a")
            .await
            .expect("workspace A active versions must load");
        assert_eq!(active_a.len(), 1);
        assert_eq!(active_a[0].skill_id, skill(SKILL_A));
        assert_eq!(active_a[0].slug, "stable-procedure");
        assert_eq!(active_a[0].version.id, VERSION_A1);
        assert_eq!(
            active_a[0].version.instruction_body,
            "Instruction version 1."
        );

        let exact_a2 = find_exact_version(&database, "ws_skill_a", VERSION_A2)
            .await
            .expect("historical version must load")
            .expect("historical version must exist");
        assert_eq!(exact_a2.version.id, VERSION_A2);
        assert_eq!(exact_a2.version.instruction_body, "Instruction version 2.");
        assert!(
            find_exact_version(&database, "ws_skill_b", VERSION_A2)
                .await
                .expect("cross-workspace exact read must execute")
                .is_none()
        );
        assert_eq!(
            list_active_versions(&database, "ws_skill_b")
                .await
                .expect("workspace B active versions must load")[0]
                .version
                .id,
            VERSION_B1
        );
    }

    #[tokio::test]
    async fn uniqueness_conflicts_surface_and_pointer_cas_never_guesses_a_target() {
        let database = database().await;
        database
            .execute_unprepared(&format!(
                "INSERT INTO self_improvement_run (
                    id, workspace_id, activation_epoch, scheduled_date_utc,
                    source_lower_exclusive, source_upper_inclusive, status,
                    learner_provider, learner_model, reviewer_provider, reviewer_model,
                    pipeline_contract_version
                 ) VALUES (
                    '{RUN_ID}', 'ws_skill_a', 1, '2030-03-02', 0, 1, 'running',
                    'openai', 'gpt-5.4', 'openai', 'gpt-5.4', 'self-improvement-v1'
                 )"
            ))
            .await
            .expect("run fixture must insert");
        let transaction = database.begin().await.expect("transaction must begin");
        insert_logical_skill(
            &transaction,
            NewAgentSkill {
                skill_id: skill(SKILL_A),
                workspace_id: "ws_skill_a".to_owned(),
                slug: "stable-procedure".to_owned(),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("logical skill A must insert");
        insert_logical_skill(
            &transaction,
            NewAgentSkill {
                skill_id: skill(SKILL_B),
                workspace_id: "ws_skill_a".to_owned(),
                slug: "second-procedure".to_owned(),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("logical skill B must insert");
        insert_immutable_version(
            &transaction,
            version(
                VERSION_A1,
                SKILL_A,
                1,
                Some(RUN_ID),
                "candidate-one",
                "fingerprint-one",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect("first version must insert");
        insert_immutable_version(
            &transaction,
            version(VERSION_B1, SKILL_B, 1, None, "candidate-b", "fingerprint-b"),
            unix_to_datetime(NOW),
        )
        .await
        .expect("skill B version must insert");

        let duplicate_slug = insert_logical_skill(
            &transaction,
            NewAgentSkill {
                skill_id: SkillId::new("CCCCCCCCCCCCCCCCCCCCC").expect("valid skill id"),
                workspace_id: "ws_skill_a".to_owned(),
                slug: "stable-procedure".to_owned(),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect_err("workspace/slug conflict must surface");
        assert!(format!("{duplicate_slug:#}").contains("failed to insert"));

        insert_immutable_version(
            &transaction,
            version(
                "444444444444444444444",
                SKILL_A,
                1,
                None,
                "candidate-version-conflict",
                "fingerprint-version-conflict",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect_err("version number conflict must surface");
        insert_immutable_version(
            &transaction,
            version(
                "555555555555555555555",
                SKILL_A,
                2,
                None,
                "candidate-fingerprint-conflict",
                "fingerprint-one",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect_err("fingerprint conflict must surface");
        insert_immutable_version(
            &transaction,
            version(
                "666666666666666666666",
                SKILL_A,
                2,
                Some(RUN_ID),
                "candidate-one",
                "fingerprint-candidate-conflict",
            ),
            unix_to_datetime(NOW),
        )
        .await
        .expect_err("run/candidate conflict must surface");

        assert!(
            set_active_version_if(
                &transaction,
                "ws_skill_a",
                &skill(SKILL_A),
                None,
                VERSION_A1,
                unix_to_datetime(NOW),
            )
            .await
            .expect("initial pointer CAS must execute")
        );
        assert!(
            !set_active_version_if(
                &transaction,
                "ws_skill_a",
                &skill(SKILL_A),
                None,
                VERSION_A1,
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("stale pointer CAS must execute")
        );
        set_active_version_if(
            &transaction,
            "ws_skill_a",
            &skill(SKILL_A),
            Some(VERSION_A1),
            VERSION_B1,
            unix_to_datetime(NOW + 1),
        )
        .await
        .expect_err("pointer must reject a version owned by another logical skill");
        transaction.commit().await.expect("transaction must commit");

        let active = list_active_versions(&database, "ws_skill_a")
            .await
            .expect("active versions must load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].version.id, VERSION_A1);
    }

    #[tokio::test]
    async fn caller_owned_create_and_update_mutations_are_atomic_and_immutable() {
        let database = database().await;
        let rolled_back = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &rolled_back,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation must apply inside outer transaction");
        rolled_back
            .rollback()
            .await
            .expect("outer transaction must roll back");
        assert!(
            list_active_versions(&database, "ws_skill_a")
                .await
                .expect("rolled-back state must query")
                .is_empty(),
            "helper must not own or commit a transaction"
        );

        let create = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &create,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation must apply");
        create.commit().await.expect("outer create must commit");

        let update = database.begin().await.expect("transaction must begin");
        let result = apply_update_in_caller_transaction(
            &update,
            UpdateAgentSkillMutation {
                workspace_id: "ws_skill_a".to_owned(),
                skill_id: skill(SKILL_A),
                expected_active_version_id: VERSION_A1.to_owned(),
                expected_slug: "stable-procedure".to_owned(),
                version: version(
                    VERSION_A2,
                    SKILL_A,
                    2,
                    None,
                    "candidate-a2",
                    "fingerprint-a2",
                ),
            },
            unix_to_datetime(NOW + 1),
        )
        .await
        .expect("update mutation must execute");
        assert_eq!(
            result,
            UpdateAgentSkillMutationResult::Applied {
                previous_version_id: VERSION_A1.to_owned(),
                resulting_version_id: VERSION_A2.to_owned(),
            }
        );
        update
            .rollback()
            .await
            .expect("outer update must roll back");
        let active = list_active_versions(&database, "ws_skill_a")
            .await
            .expect("rolled-back update must query");
        assert_eq!(active[0].version.id, VERSION_A1);
        assert!(
            find_exact_version(&database, "ws_skill_a", VERSION_A2)
                .await
                .expect("rolled-back child must query")
                .is_none(),
            "outer rollback must remove both child version and pointer change"
        );

        let update = database.begin().await.expect("transaction must begin");
        assert!(matches!(
            apply_update_in_caller_transaction(
                &update,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A1.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: version(
                        VERSION_A2,
                        SKILL_A,
                        2,
                        None,
                        "candidate-a2",
                        "fingerprint-a2",
                    ),
                },
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("replayed update mutation must execute"),
            UpdateAgentSkillMutationResult::Applied { .. }
        ));
        update.commit().await.expect("outer update must commit");

        let active = list_active_versions(&database, "ws_skill_a")
            .await
            .expect("active update must load");
        assert_eq!(active[0].version.id, VERSION_A2);
        assert_eq!(
            active[0].version.parent_version_id.as_deref(),
            Some(VERSION_A1)
        );
        assert_eq!(
            find_exact_version(&database, "ws_skill_a", VERSION_A1)
                .await
                .expect("parent must query")
                .expect("parent remains immutable")
                .version
                .instruction_body,
            "Instruction version 1."
        );
    }

    #[tokio::test]
    async fn update_mutation_enforces_stale_slug_workspace_and_fingerprint_policy() {
        let database = database().await;
        let create = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &create,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation");
        create.commit().await.expect("create commit");

        let mutation =
            |workspace_id: &str, expected_active: &str, slug: &str, fingerprint: &str| {
                let mut next_version =
                    version(VERSION_A2, SKILL_A, 2, None, "candidate-a2", fingerprint);
                next_version.parent_version_id = Some(expected_active.to_owned());
                UpdateAgentSkillMutation {
                    workspace_id: workspace_id.to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: expected_active.to_owned(),
                    expected_slug: slug.to_owned(),
                    version: next_version,
                }
            };
        let decisions = database.begin().await.expect("transaction must begin");
        assert_eq!(
            apply_update_in_caller_transaction(
                &decisions,
                mutation(
                    "ws_skill_a",
                    VERSION_A1,
                    "stable-procedure",
                    "fingerprint-a1"
                ),
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("current fingerprint decision"),
            UpdateAgentSkillMutationResult::CurrentFingerprintNoChange
        );
        assert_eq!(
            apply_update_in_caller_transaction(
                &decisions,
                mutation(
                    "ws_skill_a",
                    "999999999999999999999",
                    "stable-procedure",
                    "fingerprint-new"
                ),
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("stale decision"),
            UpdateAgentSkillMutationResult::StaleActive
        );
        assert_eq!(
            apply_update_in_caller_transaction(
                &decisions,
                mutation("ws_skill_a", VERSION_A1, "changed-slug", "fingerprint-new"),
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("slug decision"),
            UpdateAgentSkillMutationResult::Rejected("update_slug_changed")
        );
        assert_eq!(
            apply_update_in_caller_transaction(
                &decisions,
                mutation(
                    "ws_skill_b",
                    VERSION_A1,
                    "stable-procedure",
                    "fingerprint-new"
                ),
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("workspace decision"),
            UpdateAgentSkillMutationResult::Rejected("update_target_not_found")
        );
        decisions
            .rollback()
            .await
            .expect("decision transaction must roll back");
        assert!(
            find_exact_version(&database, "ws_skill_a", VERSION_A2)
                .await
                .expect("rejected update state must query")
                .is_none()
        );
    }

    #[tokio::test]
    async fn update_fingerprint_policy_distinguishes_parent_and_older_history() {
        let database = database().await;
        let create = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &create,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation");
        create.commit().await.expect("create commit");

        let update = database.begin().await.expect("transaction must begin");
        assert!(matches!(
            apply_update_in_caller_transaction(
                &update,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A1.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: version(
                        VERSION_A2,
                        SKILL_A,
                        2,
                        None,
                        "candidate-a2",
                        "fingerprint-a2",
                    ),
                },
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("first update"),
            UpdateAgentSkillMutationResult::Applied { .. }
        ));
        update.commit().await.expect("first update commit");

        let parent_match = database.begin().await.expect("transaction must begin");
        let mut candidate = version(
            VERSION_A3,
            SKILL_A,
            3,
            None,
            "candidate-parent-match",
            "fingerprint-a1",
        );
        candidate.parent_version_id = Some(VERSION_A2.to_owned());
        assert_eq!(
            apply_update_in_caller_transaction(
                &parent_match,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: candidate,
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("parent fingerprint decision"),
            UpdateAgentSkillMutationResult::ExactParentFingerprintRequiresRollback {
                parent_version_id: VERSION_A1.to_owned(),
            }
        );
        parent_match
            .rollback()
            .await
            .expect("parent decision transaction rollback");

        let update = database.begin().await.expect("transaction must begin");
        let mut candidate = version(
            VERSION_A3,
            SKILL_A,
            3,
            None,
            "candidate-a3",
            "fingerprint-a3",
        );
        candidate.parent_version_id = Some(VERSION_A2.to_owned());
        assert!(matches!(
            apply_update_in_caller_transaction(
                &update,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: candidate,
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("second update"),
            UpdateAgentSkillMutationResult::Applied { .. }
        ));
        update.commit().await.expect("second update commit");

        let history_match = database.begin().await.expect("transaction must begin");
        let mut candidate = version(
            VERSION_A4,
            SKILL_A,
            4,
            None,
            "candidate-history-match",
            "fingerprint-a1",
        );
        candidate.parent_version_id = Some(VERSION_A3.to_owned());
        assert_eq!(
            apply_update_in_caller_transaction(
                &history_match,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A3.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: candidate,
                },
                unix_to_datetime(NOW + 3),
            )
            .await
            .expect("historical fingerprint decision"),
            UpdateAgentSkillMutationResult::HistoricalFingerprintNoChange {
                existing_version_id: VERSION_A1.to_owned(),
            }
        );
        history_match
            .rollback()
            .await
            .expect("historical decision transaction rollback");
        assert!(
            find_exact_version(&database, "ws_skill_a", VERSION_A4)
                .await
                .expect("historical no-op state must query")
                .is_none()
        );
    }

    #[tokio::test]
    async fn exact_parent_rollback_changes_only_pointer_and_obeys_outer_transaction() {
        let database = database().await;
        let create = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &create,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation");
        create.commit().await.expect("create commit");
        let update = database.begin().await.expect("transaction must begin");
        assert!(matches!(
            apply_update_in_caller_transaction(
                &update,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A1.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: version(
                        VERSION_A2,
                        SKILL_A,
                        2,
                        None,
                        "candidate-a2",
                        "fingerprint-a2",
                    ),
                },
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("update mutation"),
            UpdateAgentSkillMutationResult::Applied { .. }
        ));
        update.commit().await.expect("update commit");

        let rollback = database.begin().await.expect("transaction must begin");
        assert_eq!(
            apply_rollback_in_caller_transaction(
                &rollback,
                RollbackAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    target_parent_version_id: VERSION_A1.to_owned(),
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("rollback mutation"),
            RollbackAgentSkillMutationResult::Applied {
                previous_version_id: VERSION_A2.to_owned(),
                resulting_version_id: VERSION_A1.to_owned(),
            }
        );
        let inside = list_active_versions(&rollback, "ws_skill_a")
            .await
            .expect("transaction-local active version");
        assert_eq!(inside[0].version.id, VERSION_A1);
        assert_eq!(inside[0].version.display_name, "Skill version 1");
        assert_eq!(
            inside[0].version.skill_markdown,
            "---\nname: example\n---\n\nInstruction version 1."
        );
        assert_eq!(inside[0].version.instruction_body, "Instruction version 1.");
        rollback
            .rollback()
            .await
            .expect("outer rollback must roll back pointer");
        assert_eq!(
            list_active_versions(&database, "ws_skill_a")
                .await
                .expect("active after rollback")[0]
                .version
                .id,
            VERSION_A2
        );

        let rollback = database.begin().await.expect("transaction must begin");
        assert!(matches!(
            apply_rollback_in_caller_transaction(
                &rollback,
                RollbackAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    target_parent_version_id: VERSION_A1.to_owned(),
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("committed rollback mutation"),
            RollbackAgentSkillMutationResult::Applied { .. }
        ));
        rollback.commit().await.expect("outer rollback commit");
        let restored = list_active_versions(&database, "ws_skill_a")
            .await
            .expect("restored active version");
        assert_eq!(restored[0].version.id, VERSION_A1);
        assert_eq!(restored[0].version.display_name, "Skill version 1");
        assert_eq!(
            restored[0].version.when_to_use,
            "Use for the tested procedure."
        );
        assert_eq!(
            restored[0].version.when_not_to_use,
            "Do not use outside the tested procedure."
        );
        assert_eq!(
            agent_skill_version::Entity::find()
                .filter(agent_skill_version::Column::SkillId.eq(SKILL_A))
                .count(&database)
                .await
                .expect("version count"),
            2,
            "rollback must not copy or create a version"
        );
    }

    #[tokio::test]
    async fn rollback_rejects_stale_cross_owned_and_non_parent_targets_without_writes() {
        let database = database().await;
        let create = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &create,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_A),
                    workspace_id: "ws_skill_a".to_owned(),
                    slug: "stable-procedure".to_owned(),
                },
                version: version(
                    VERSION_A1,
                    SKILL_A,
                    1,
                    None,
                    "candidate-a1",
                    "fingerprint-a1",
                ),
            },
            unix_to_datetime(NOW),
        )
        .await
        .expect("create mutation");
        create.commit().await.expect("create commit");
        let update = database.begin().await.expect("transaction must begin");
        assert!(matches!(
            apply_update_in_caller_transaction(
                &update,
                UpdateAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A1.to_owned(),
                    expected_slug: "stable-procedure".to_owned(),
                    version: version(
                        VERSION_A2,
                        SKILL_A,
                        2,
                        None,
                        "candidate-a2",
                        "fingerprint-a2",
                    ),
                },
                unix_to_datetime(NOW + 1),
            )
            .await
            .expect("update mutation"),
            UpdateAgentSkillMutationResult::Applied { .. }
        ));
        update.commit().await.expect("update commit");
        let other = database.begin().await.expect("transaction must begin");
        apply_create_in_caller_transaction(
            &other,
            CreateAgentSkillMutation {
                skill: NewAgentSkill {
                    skill_id: skill(SKILL_B),
                    workspace_id: "ws_skill_b".to_owned(),
                    slug: "other-procedure".to_owned(),
                },
                version: version(
                    VERSION_B1,
                    SKILL_B,
                    1,
                    None,
                    "candidate-b1",
                    "fingerprint-b1",
                ),
            },
            unix_to_datetime(NOW + 1),
        )
        .await
        .expect("cross-owned skill create");
        other.commit().await.expect("cross-owned skill commit");

        let decisions = database.begin().await.expect("transaction must begin");
        assert_eq!(
            apply_rollback_in_caller_transaction(
                &decisions,
                RollbackAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A1.to_owned(),
                    target_parent_version_id: VERSION_B1.to_owned(),
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("stale rollback"),
            RollbackAgentSkillMutationResult::StaleActive
        );
        assert_eq!(
            apply_rollback_in_caller_transaction(
                &decisions,
                RollbackAgentSkillMutation {
                    workspace_id: "ws_skill_b".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    target_parent_version_id: VERSION_A1.to_owned(),
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("cross-workspace rollback"),
            RollbackAgentSkillMutationResult::Rejected("rollback_target_not_found")
        );
        assert_eq!(
            apply_rollback_in_caller_transaction(
                &decisions,
                RollbackAgentSkillMutation {
                    workspace_id: "ws_skill_a".to_owned(),
                    skill_id: skill(SKILL_A),
                    expected_active_version_id: VERSION_A2.to_owned(),
                    target_parent_version_id: VERSION_B1.to_owned(),
                },
                unix_to_datetime(NOW + 2),
            )
            .await
            .expect("non-parent rollback"),
            RollbackAgentSkillMutationResult::Rejected("rollback_target_not_exact_parent")
        );
        decisions
            .rollback()
            .await
            .expect("rejected decisions rollback");
        assert_eq!(
            list_active_versions(&database, "ws_skill_a")
                .await
                .expect("unchanged active pointer")[0]
                .version
                .id,
            VERSION_A2
        );
        assert_eq!(
            agent_skill_version::Entity::find()
                .filter(agent_skill_version::Column::SkillId.eq(SKILL_A))
                .count(&database)
                .await
                .expect("version count"),
            2
        );
        assert_eq!(
            list_active_versions(&database, "ws_skill_b")
                .await
                .expect("cross-owned skill remains intact")[0]
                .version
                .id,
            VERSION_B1
        );
    }
}
