use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_retryable_database_conflict,
    retry_with_backoff,
};
use pioneer_protocol::{SkillId, SkillPackId};
use sea_orm::{ConnectionTrait, TransactionSession, TransactionTrait};

use crate::skill_audit_event::SkillAuditEventRecord;
use crate::skill_pack_installation::{
    SkillPackInstallationRecord, skill_pack_installation_record_from_model, unix_to_datetime,
};
use crate::skill_records::{SkillInstallationRecord, WorkspaceSkillPolicyRecord};
use crate::{
    skill_audit_event, skill_installation, skill_pack_installation, skill_upload_session,
    skill_workspace_policy,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillPackChildDiff {
    pub retained: Vec<SkillInstallationRecord>,
    pub added: Vec<SkillInstallationRecord>,
    pub removed: Vec<SkillId>,
}

pub fn skill_installation_record_from_model(
    model: pioneer_entity::skill_installation::Model,
) -> Result<SkillInstallationRecord> {
    let skill_id = SkillId::new(model.id.clone()).with_context(|| {
        format!(
            "skill installation primary key `{}` is not a valid SkillId",
            model.id
        )
    })?;
    let pack_id = model
        .pack_id
        .map(|pack_id| {
            SkillPackId::new(pack_id.clone()).with_context(|| {
                format!("skill installation pack key `{pack_id}` is not a valid SkillPackId")
            })
        })
        .transpose()?;
    if pack_id.is_some() != model.pack_member_key.is_some() {
        bail!("skill installation `{skill_id}` has incomplete pack membership");
    }
    Ok(SkillInstallationRecord {
        skill_id,
        owner: model.owner,
        slug: model.slug,
        version: model.version,
        source_kind: model.source_kind,
        scope_key: model.scope_key,
        source_ref: model.source_ref,
        install_path: model.install_path,
        trust_level: model.trust_level,
        fingerprint: model.fingerprint,
        updated_at_unix: model.updated_at.timestamp(),
        pack_id,
        pack_member_key: model.pack_member_key,
    })
}

const MAX_ATOMIC_SKILL_PACK_MEMBERS: usize = 256;
const MAX_ATOMIC_SKILL_PACK_AUDIT_EVENTS: usize = 512;

pub fn validate_atomic_skill_audit_bound(audit_records: &[SkillAuditEventRecord]) -> Result<()> {
    if audit_records.len() > MAX_ATOMIC_SKILL_PACK_AUDIT_EVENTS {
        bail!("atomic skill pack audit set exceeds {MAX_ATOMIC_SKILL_PACK_AUDIT_EVENTS} events");
    }
    Ok(())
}

pub fn validate_skill_pack_child(
    parent: &SkillPackInstallationRecord,
    child: &SkillInstallationRecord,
) -> Result<()> {
    if child.scope_key != parent.scope_key {
        bail!(
            "skill `{}` scope `{}` does not match pack `{}` scope `{}`",
            child.skill_id,
            child.scope_key,
            parent.pack_id,
            parent.scope_key
        );
    }
    if child.pack_id.as_ref() != Some(&parent.pack_id) {
        bail!(
            "skill `{}` does not belong to pack `{}`",
            child.skill_id,
            parent.pack_id
        );
    }
    if child.pack_member_key.is_none() {
        bail!("skill `{}` is missing its pack member key", child.skill_id);
    }
    if child.source_kind != parent.source_kind {
        bail!(
            "skill `{}` source `{}` does not match pack `{}` source `{}`",
            child.skill_id,
            child.source_kind,
            parent.pack_id,
            parent.source_kind
        );
    }
    Ok(())
}

fn validate_skill_pack_children_scope(
    parent: &SkillPackInstallationRecord,
    children: &[SkillInstallationRecord],
) -> Result<()> {
    if children.len() > MAX_ATOMIC_SKILL_PACK_MEMBERS {
        bail!("skill pack exceeds {MAX_ATOMIC_SKILL_PACK_MEMBERS} members");
    }
    for child in children {
        validate_skill_pack_child(parent, child)?;
    }
    Ok(())
}

fn validate_skill_pack_child_diff(
    parent: &SkillPackInstallationRecord,
    current_children: &[SkillInstallationRecord],
    diff: &SkillPackChildDiff,
) -> Result<()> {
    validate_skill_pack_children_scope(parent, current_children)?;
    validate_skill_pack_children_scope(parent, diff.retained.as_slice())?;
    validate_skill_pack_children_scope(parent, diff.added.as_slice())?;
    if diff.retained.len().saturating_add(diff.added.len()) > MAX_ATOMIC_SKILL_PACK_MEMBERS
        || diff.retained.len().saturating_add(diff.removed.len()) > MAX_ATOMIC_SKILL_PACK_MEMBERS
    {
        bail!("skill pack update exceeds {MAX_ATOMIC_SKILL_PACK_MEMBERS} members");
    }

    let current_by_id = current_children
        .iter()
        .map(|child| (child.skill_id.clone(), child))
        .collect::<HashMap<_, _>>();
    if current_by_id.len() != current_children.len() {
        bail!("skill pack contains duplicate SkillId values");
    }
    let mut accounted = HashSet::new();
    for retained in &diff.retained {
        let current = current_by_id.get(&retained.skill_id).with_context(|| {
            format!(
                "retained skill `{}` is not a current child of pack `{}`",
                retained.skill_id, parent.pack_id
            )
        })?;
        if current.pack_id != retained.pack_id
            || current.pack_member_key != retained.pack_member_key
            || !accounted.insert(retained.skill_id.clone())
        {
            bail!(
                "retained skill `{}` has an invalid pack update identity",
                retained.skill_id
            );
        }
    }
    for removed in &diff.removed {
        if !current_by_id.contains_key(removed) || !accounted.insert(removed.clone()) {
            bail!("removed skill `{removed}` is invalid for pack update");
        }
    }
    if accounted.len() != current_by_id.len() {
        bail!("pack update diff does not account for every current child");
    }
    for added in &diff.added {
        if current_by_id.contains_key(&added.skill_id) || !accounted.insert(added.skill_id.clone())
        {
            bail!("added skill `{}` collides in pack update", added.skill_id);
        }
    }
    Ok(())
}

pub(crate) async fn retry_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

/// Skill pack operations use the caller's scoped connection for writer admission.
pub struct SkillPackRepository<'a, C> {
    connection: &'a C,
}

impl<'a, C: ConnectionTrait + TransactionTrait> SkillPackRepository<'a, C> {
    pub fn new(connection: &'a C) -> Self {
        Self { connection }
    }

    pub async fn find_skill_pack_installation(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<Option<SkillPackInstallationRecord>> {
        skill_pack_installation::find_skill_pack_installation(self.connection, scope_key, pack_id)
            .await?
            .map(skill_pack_installation_record_from_model)
            .transpose()
    }

    pub async fn find_skill_pack_installation_by_id(
        &self,
        pack_id: &SkillPackId,
    ) -> Result<Option<SkillPackInstallationRecord>> {
        skill_pack_installation::find_skill_pack_installation_by_id(self.connection, pack_id)
            .await?
            .map(skill_pack_installation_record_from_model)
            .transpose()
    }

    pub async fn list_skill_pack_installations(
        &self,
        scope_key: &str,
    ) -> Result<Vec<SkillPackInstallationRecord>> {
        skill_pack_installation::list_skill_pack_installations(self.connection, scope_key)
            .await?
            .into_iter()
            .map(skill_pack_installation_record_from_model)
            .collect()
    }

    pub async fn list_skill_installations_for_pack(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<Vec<SkillInstallationRecord>> {
        skill_installation::list_skill_installations_for_pack(self.connection, scope_key, pack_id)
            .await?
            .into_iter()
            .map(skill_installation_record_from_model)
            .collect()
    }

    pub async fn update_skill_pack_installation_name(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
        name: &str,
        updated_at_unix: i64,
    ) -> Result<bool> {
        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            scope_key,
            pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        let expected_parent =
            skill_pack_installation_record_from_model(expected_parent_model.clone())?;
        let expected_children_models =
            skill_installation::list_skill_installations_by_pack_id(self.connection, pack_id)
                .await?;
        let expected_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        validate_skill_pack_children_scope(&expected_parent, expected_children.as_slice())?;
        let updated_at = unix_to_datetime(updated_at_unix);
        retry_write(|| {
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack name update transaction")?;
                let result: Result<bool> = async {
                    let Some(parent_model) = skill_pack_installation::lock_skill_pack_installation(
                        &transaction,
                        scope_key,
                        pack_id,
                    )
                    .await?
                    else {
                        return Ok(false);
                    };
                    if parent_model != expected_parent_model {
                        return Ok(false);
                    }
                    let children_models = skill_installation::lock_skill_installations_by_pack_id(
                        &transaction,
                        pack_id,
                    )
                    .await?;
                    if children_models != expected_children_models {
                        return Ok(false);
                    }
                    skill_pack_installation::update_skill_pack_installation_name(
                        &transaction,
                        scope_key,
                        pack_id,
                        name,
                        updated_at,
                    )
                    .await
                }
                .await;
                match result {
                    Ok(updated) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack name update")?;
                        Ok(updated)
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

    pub async fn delete_skill_pack_installation(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<bool> {
        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            scope_key,
            pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        let expected_parent =
            skill_pack_installation_record_from_model(expected_parent_model.clone())?;
        let expected_children_models =
            skill_installation::list_skill_installations_by_pack_id(self.connection, pack_id)
                .await?;
        let expected_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        validate_skill_pack_children_scope(&expected_parent, expected_children.as_slice())?;
        retry_write(|| {
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack deletion transaction")?;
                let result: Result<bool> = async {
                    let Some(parent_model) = skill_pack_installation::lock_skill_pack_installation(
                        &transaction,
                        scope_key,
                        pack_id,
                    )
                    .await?
                    else {
                        return Ok(false);
                    };
                    if parent_model != expected_parent_model {
                        return Ok(false);
                    }
                    let children_models = skill_installation::lock_skill_installations_by_pack_id(
                        &transaction,
                        pack_id,
                    )
                    .await?;
                    if children_models != expected_children_models {
                        return Ok(false);
                    }
                    skill_pack_installation::delete_skill_pack_installation(
                        &transaction,
                        scope_key,
                        pack_id,
                    )
                    .await
                }
                .await;
                match result {
                    Ok(deleted) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack deletion")?;
                        Ok(deleted)
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

    pub async fn insert_skill_pack_installation_with_children(
        &self,
        parent: &SkillPackInstallationRecord,
        children: &[SkillInstallationRecord],
    ) -> Result<()> {
        validate_skill_pack_children_scope(parent, children)?;
        let prepared_parent = skill_pack_installation::prepare_skill_pack_installation(parent);
        let prepared_children = children
            .iter()
            .map(|child| {
                let timestamp = unix_to_datetime(child.updated_at_unix);
                skill_installation::prepare_skill_installation(child, timestamp, timestamp)
            })
            .collect::<Vec<_>>();
        retry_write(|| {
            let prepared_parent = prepared_parent.clone();
            let prepared_children = prepared_children.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack insertion transaction")?;
                let result: Result<()> = async {
                    skill_pack_installation::insert_prepared_skill_pack_installation(
                        &transaction,
                        prepared_parent,
                    )
                    .await?;
                    skill_installation::insert_prepared_skill_installations(
                        &transaction,
                        prepared_children.as_slice(),
                    )
                    .await?;
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => transaction
                        .commit()
                        .await
                        .context("failed to commit skill pack insertion"),
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }

    pub async fn install_skill_pack_lifecycle(
        &self,
        parent: &SkillPackInstallationRecord,
        children: &[SkillInstallationRecord],
        policies: &[WorkspaceSkillPolicyRecord],
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        validate_skill_pack_children_scope(parent, children)?;
        validate_atomic_skill_audit_bound(audit_records)?;
        let child_ids = children
            .iter()
            .map(|child| child.skill_id.clone())
            .collect::<HashSet<_>>();
        if child_ids.len() != children.len() {
            bail!("skill pack install children must have unique SkillId values");
        }
        let policy_ids = policies
            .iter()
            .map(|policy| {
                if policy.workspace_id != parent.scope_key {
                    bail!(
                        "skill policy `{}` scope does not match pack `{}`",
                        policy.skill_id,
                        parent.pack_id
                    );
                }
                Ok(policy.skill_id.clone())
            })
            .collect::<Result<HashSet<_>>>()?;
        if policy_ids != child_ids || policies.len() != children.len() {
            bail!("skill pack install requires exactly one policy per child");
        }
        let audit_ids = audit_records
            .iter()
            .map(|audit| audit.skill_id.clone())
            .collect::<HashSet<_>>();
        if audit_ids != child_ids {
            bail!("skill pack install requires child audit coverage");
        }
        let now = unix_to_datetime(event_timestamp_secs);
        let prepared_parent = skill_pack_installation::prepare_skill_pack_installation(parent);
        let prepared_children = children
            .iter()
            .map(|child| skill_installation::prepare_skill_installation(child, now, now))
            .collect::<Vec<_>>();
        let prepared_policies = policies
            .iter()
            .map(|policy| skill_workspace_policy::prepare_workspace_skill_policy(policy, now, now))
            .collect::<Vec<_>>();
        let prepared_audit_records =
            skill_audit_event::prepare_skill_audit_events(None, audit_records);
        let upload_id = upload_id.to_owned();
        retry_write(|| {
            let prepared_parent = prepared_parent.clone();
            let prepared_children = prepared_children.clone();
            let prepared_policies = prepared_policies.clone();
            let prepared_audit_records = prepared_audit_records.clone();
            let upload_id = upload_id.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack lifecycle install transaction")?;
                let result: Result<bool> = async {
                    skill_pack_installation::insert_prepared_skill_pack_installation(
                        &transaction,
                        prepared_parent,
                    )
                    .await?;
                    skill_installation::insert_prepared_skill_installations(
                        &transaction,
                        prepared_children.as_slice(),
                    )
                    .await?;
                    skill_workspace_policy::upsert_prepared_workspace_skill_policies(
                        &transaction,
                        prepared_policies.as_slice(),
                    )
                    .await?;
                    skill_audit_event::insert_prepared_skill_audit_events(
                        &transaction,
                        prepared_audit_records,
                    )
                    .await?;
                    if !skill_upload_session::transition_skill_upload_status(
                        &transaction,
                        upload_id.as_str(),
                        &["finalized"],
                        "consumed",
                        None,
                        Some(event_timestamp_secs),
                        None,
                        now,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(true) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack lifecycle install")?;
                        Ok(true)
                    }
                    Ok(false) => {
                        transaction
                            .rollback()
                            .await
                            .context("failed to roll back conflicted skill pack install")?;
                        Ok(false)
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

    pub async fn update_skill_pack_installation_with_child_diff(
        &self,
        parent: &SkillPackInstallationRecord,
        diff: &SkillPackChildDiff,
    ) -> Result<bool> {
        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            parent.scope_key.as_str(),
            &parent.pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        let expected_parent_record =
            skill_pack_installation_record_from_model(expected_parent_model.clone())?;
        if expected_parent_record.source_kind != parent.source_kind {
            bail!(
                "skill pack `{}` source kind cannot be changed",
                parent.pack_id
            );
        }
        let expected_children_models = skill_installation::list_skill_installations_by_pack_id(
            self.connection,
            &parent.pack_id,
        )
        .await?;
        let expected_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        validate_skill_pack_child_diff(parent, expected_children.as_slice(), diff)?;
        let removed_skill_ids = diff.removed.clone();
        let prepared_children = diff
            .retained
            .iter()
            .chain(diff.added.iter())
            .map(|child| {
                let timestamp = unix_to_datetime(child.updated_at_unix);
                skill_installation::prepare_skill_installation(child, timestamp, timestamp)
            })
            .collect::<Vec<_>>();
        let parent_updated_at = unix_to_datetime(parent.updated_at_unix);
        let parent = parent.clone();
        retry_write(|| {
            let parent = parent.clone();
            let removed_skill_ids = removed_skill_ids.clone();
            let prepared_children = prepared_children.clone();
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack update transaction")?;
                let result: Result<bool> = async {
                    let Some(current_parent_model) =
                        skill_pack_installation::lock_skill_pack_installation(
                            &transaction,
                            parent.scope_key.as_str(),
                            &parent.pack_id,
                        )
                        .await?
                    else {
                        return Ok(false);
                    };
                    if current_parent_model != expected_parent_model {
                        return Ok(false);
                    }

                    let current_children_models =
                        skill_installation::lock_skill_installations_by_pack_id(
                            &transaction,
                            &parent.pack_id,
                        )
                        .await?;
                    if current_children_models != expected_children_models {
                        return Ok(false);
                    }

                    let deleted = skill_installation::delete_skill_installations(
                        &transaction,
                        removed_skill_ids.as_slice(),
                    )
                    .await?;
                    if deleted != u64::try_from(removed_skill_ids.len()).unwrap_or(u64::MAX) {
                        bail!("one or more pack children disappeared during update");
                    }
                    skill_installation::upsert_prepared_pack_skill_installations(
                        &transaction,
                        prepared_children.as_slice(),
                    )
                    .await?;
                    if !skill_pack_installation::update_skill_pack_installation_name(
                        &transaction,
                        parent.scope_key.as_str(),
                        &parent.pack_id,
                        parent.name.as_str(),
                        parent_updated_at,
                    )
                    .await?
                    {
                        bail!("skill pack `{}` changed during update", parent.pack_id);
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(updated) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack update")?;
                        Ok(updated)
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

    pub async fn update_skill_pack_lifecycle(
        &self,
        expected_parent: &SkillPackInstallationRecord,
        expected_children: &[SkillInstallationRecord],
        parent: &SkillPackInstallationRecord,
        diff: &SkillPackChildDiff,
        added_policies: &[WorkspaceSkillPolicyRecord],
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        if parent.pack_id != expected_parent.pack_id
            || parent.scope_key != expected_parent.scope_key
            || parent.source_kind != expected_parent.source_kind
            || parent.created_at_unix != expected_parent.created_at_unix
        {
            bail!("skill pack update cannot change parent identity or source");
        }
        validate_skill_pack_children_scope(expected_parent, expected_children)?;
        validate_skill_pack_child_diff(parent, expected_children, diff)?;
        validate_atomic_skill_audit_bound(audit_records)?;
        let added_ids = diff
            .added
            .iter()
            .map(|child| child.skill_id.clone())
            .collect::<HashSet<_>>();
        let policy_ids = added_policies
            .iter()
            .map(|policy| {
                if policy.workspace_id != parent.scope_key {
                    bail!(
                        "added skill policy `{}` scope does not match pack",
                        policy.skill_id
                    );
                }
                Ok(policy.skill_id.clone())
            })
            .collect::<Result<HashSet<_>>>()?;
        if policy_ids != added_ids || added_policies.len() != diff.added.len() {
            bail!("pack update requires exactly one default policy per added child");
        }
        let affected_ids = diff
            .retained
            .iter()
            .map(|child| child.skill_id.clone())
            .chain(diff.added.iter().map(|child| child.skill_id.clone()))
            .chain(diff.removed.iter().cloned())
            .collect::<HashSet<_>>();
        let audit_ids = audit_records
            .iter()
            .map(|audit| audit.skill_id.clone())
            .collect::<HashSet<_>>();
        if audit_ids != affected_ids {
            bail!("skill pack update requires affected-child audit coverage");
        }

        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            expected_parent.scope_key.as_str(),
            &expected_parent.pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        if skill_pack_installation_record_from_model(expected_parent_model.clone())?
            != *expected_parent
        {
            return Ok(false);
        }
        let expected_children_models = skill_installation::list_skill_installations_by_pack_id(
            self.connection,
            &expected_parent.pack_id,
        )
        .await?;
        let current_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        if current_children != expected_children {
            return Ok(false);
        }
        let now = unix_to_datetime(event_timestamp_secs);
        let removed_skill_ids = diff.removed.clone();
        let mut prepared_children = diff
            .retained
            .iter()
            .map(|child| {
                let timestamp = unix_to_datetime(child.updated_at_unix);
                skill_installation::prepare_skill_installation(child, timestamp, timestamp)
            })
            .collect::<Vec<_>>();
        prepared_children.extend(
            diff.added
                .iter()
                .map(|child| skill_installation::prepare_skill_installation(child, now, now)),
        );
        let prepared_added_policies = added_policies
            .iter()
            .map(|policy| skill_workspace_policy::prepare_workspace_skill_policy(policy, now, now))
            .collect::<Vec<_>>();
        let expected_parent = expected_parent.clone();
        let parent = parent.clone();
        let prepared_audit_records =
            skill_audit_event::prepare_skill_audit_events(None, audit_records);
        let upload_id = upload_id.to_owned();
        retry_write(|| {
            let expected_parent = expected_parent.clone();
            let parent = parent.clone();
            let removed_skill_ids = removed_skill_ids.clone();
            let prepared_children = prepared_children.clone();
            let prepared_added_policies = prepared_added_policies.clone();
            let prepared_audit_records = prepared_audit_records.clone();
            let upload_id = upload_id.clone();
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack lifecycle update transaction")?;
                let result: Result<bool> = async {
                    let Some(current_parent_model) =
                        skill_pack_installation::lock_skill_pack_installation(
                            &transaction,
                            expected_parent.scope_key.as_str(),
                            &expected_parent.pack_id,
                        )
                        .await?
                    else {
                        return Ok(false);
                    };
                    if current_parent_model != expected_parent_model {
                        return Ok(false);
                    }
                    let current_children_models =
                        skill_installation::lock_skill_installations_by_pack_id(
                            &transaction,
                            &expected_parent.pack_id,
                        )
                        .await?;
                    if current_children_models != expected_children_models {
                        return Ok(false);
                    }
                    let deleted = skill_installation::delete_skill_installations(
                        &transaction,
                        removed_skill_ids.as_slice(),
                    )
                    .await?;
                    if deleted != u64::try_from(removed_skill_ids.len()).unwrap_or(u64::MAX) {
                        return Ok(false);
                    }
                    skill_workspace_policy::delete_workspace_skill_policies(
                        &transaction,
                        parent.scope_key.as_str(),
                        removed_skill_ids.as_slice(),
                    )
                    .await?;
                    skill_installation::upsert_prepared_pack_skill_installations(
                        &transaction,
                        prepared_children.as_slice(),
                    )
                    .await?;
                    skill_workspace_policy::upsert_prepared_workspace_skill_policies(
                        &transaction,
                        prepared_added_policies.as_slice(),
                    )
                    .await?;
                    if !skill_pack_installation::update_skill_pack_installation_name(
                        &transaction,
                        parent.scope_key.as_str(),
                        &parent.pack_id,
                        parent.name.as_str(),
                        now,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                    skill_audit_event::insert_prepared_skill_audit_events(
                        &transaction,
                        prepared_audit_records,
                    )
                    .await?;
                    if !skill_upload_session::transition_skill_upload_status(
                        &transaction,
                        upload_id.as_str(),
                        &["finalized"],
                        "consumed",
                        None,
                        Some(event_timestamp_secs),
                        None,
                        now,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(true) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack lifecycle update")?;
                        Ok(true)
                    }
                    Ok(false) => {
                        transaction
                            .rollback()
                            .await
                            .context("failed to roll back conflicted skill pack update")?;
                        Ok(false)
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

    pub async fn uninstall_skill_pack_lifecycle(
        &self,
        expected_parent: &SkillPackInstallationRecord,
        expected_children: &[SkillInstallationRecord],
        audit_records: &[SkillAuditEventRecord],
    ) -> Result<bool> {
        validate_skill_pack_children_scope(expected_parent, expected_children)?;
        validate_atomic_skill_audit_bound(audit_records)?;
        let child_ids = expected_children
            .iter()
            .map(|child| child.skill_id.clone())
            .collect::<HashSet<_>>();
        if child_ids.len() != expected_children.len() {
            bail!("skill pack uninstall children must have unique SkillId values");
        }
        let audit_ids = audit_records
            .iter()
            .map(|audit| audit.skill_id.clone())
            .collect::<HashSet<_>>();
        if audit_ids != child_ids {
            bail!("skill pack uninstall requires child audit coverage");
        }
        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            expected_parent.scope_key.as_str(),
            &expected_parent.pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        if skill_pack_installation_record_from_model(expected_parent_model.clone())?
            != *expected_parent
        {
            return Ok(false);
        }
        let expected_children_models = skill_installation::list_skill_installations_by_pack_id(
            self.connection,
            &expected_parent.pack_id,
        )
        .await?;
        let current_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        if current_children != expected_children {
            return Ok(false);
        }
        let expected_parent = expected_parent.clone();
        let expected_child_ids = expected_children
            .iter()
            .map(|child| child.skill_id.clone())
            .collect::<Vec<_>>();
        let prepared_audit_records =
            skill_audit_event::prepare_skill_audit_events(None, audit_records);
        retry_write(|| {
            let expected_parent = expected_parent.clone();
            let expected_child_ids = expected_child_ids.clone();
            let prepared_audit_records = prepared_audit_records.clone();
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack lifecycle uninstall transaction")?;
                let result: Result<bool> = async {
                    let Some(current_parent_model) =
                        skill_pack_installation::lock_skill_pack_installation(
                            &transaction,
                            expected_parent.scope_key.as_str(),
                            &expected_parent.pack_id,
                        )
                        .await?
                    else {
                        return Ok(false);
                    };
                    if current_parent_model != expected_parent_model {
                        return Ok(false);
                    }
                    let current_children_models =
                        skill_installation::lock_skill_installations_by_pack_id(
                            &transaction,
                            &expected_parent.pack_id,
                        )
                        .await?;
                    if current_children_models != expected_children_models {
                        return Ok(false);
                    }
                    skill_workspace_policy::delete_workspace_skill_policies(
                        &transaction,
                        expected_parent.scope_key.as_str(),
                        expected_child_ids.as_slice(),
                    )
                    .await?;
                    let deleted = skill_installation::delete_skill_installations(
                        &transaction,
                        expected_child_ids.as_slice(),
                    )
                    .await?;
                    if deleted != u64::try_from(expected_child_ids.len()).unwrap_or(u64::MAX) {
                        return Ok(false);
                    }
                    skill_audit_event::insert_prepared_skill_audit_events(
                        &transaction,
                        prepared_audit_records,
                    )
                    .await?;
                    if !skill_pack_installation::delete_skill_pack_installation(
                        &transaction,
                        expected_parent.scope_key.as_str(),
                        &expected_parent.pack_id,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(true) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack lifecycle uninstall")?;
                        Ok(true)
                    }
                    Ok(false) => {
                        transaction
                            .rollback()
                            .await
                            .context("failed to roll back conflicted skill pack uninstall")?;
                        Ok(false)
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

    pub async fn delete_skill_pack_installation_with_children(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<bool> {
        let Some(expected_parent_model) = skill_pack_installation::find_skill_pack_installation(
            self.connection,
            scope_key,
            pack_id,
        )
        .await?
        else {
            return Ok(false);
        };
        let expected_parent =
            skill_pack_installation_record_from_model(expected_parent_model.clone())?;
        let expected_children_models =
            skill_installation::list_skill_installations_by_pack_id(self.connection, pack_id)
                .await?;
        let expected_children = expected_children_models
            .iter()
            .cloned()
            .map(skill_installation_record_from_model)
            .collect::<Result<Vec<_>>>()?;
        validate_skill_pack_children_scope(&expected_parent, expected_children.as_slice())?;
        let expected_child_ids = expected_children
            .iter()
            .map(|child| child.skill_id.clone())
            .collect::<Vec<_>>();
        let scope_key = scope_key.to_owned();
        let pack_id = pack_id.clone();
        retry_write(|| {
            let scope_key = scope_key.clone();
            let pack_id = pack_id.clone();
            let expected_parent_model = expected_parent_model.clone();
            let expected_children_models = expected_children_models.clone();
            let expected_child_ids = expected_child_ids.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin skill pack removal transaction")?;
                let result: Result<bool> = async {
                    let Some(parent_model) = skill_pack_installation::lock_skill_pack_installation(
                        &transaction,
                        scope_key.as_str(),
                        &pack_id,
                    )
                    .await?
                    else {
                        return Ok(false);
                    };
                    if parent_model != expected_parent_model {
                        return Ok(false);
                    }
                    let children_models = skill_installation::lock_skill_installations_by_pack_id(
                        &transaction,
                        &pack_id,
                    )
                    .await?;
                    if children_models != expected_children_models {
                        return Ok(false);
                    }
                    let deleted = skill_installation::delete_skill_installations(
                        &transaction,
                        expected_child_ids.as_slice(),
                    )
                    .await?;
                    if deleted != u64::try_from(expected_child_ids.len()).unwrap_or(u64::MAX) {
                        bail!("one or more pack children disappeared during removal");
                    }
                    if !skill_pack_installation::delete_skill_pack_installation(
                        &transaction,
                        scope_key.as_str(),
                        &pack_id,
                    )
                    .await?
                    {
                        bail!("skill pack `{pack_id}` disappeared during removal");
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(deleted) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit skill pack removal")?;
                        Ok(deleted)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_pack_record(id: char, name: &str, scope_key: &str) -> SkillPackInstallationRecord {
        SkillPackInstallationRecord {
            pack_id: SkillPackId::new(id.to_string().repeat(21)).expect("valid pack id"),
            name: name.to_owned(),
            scope_key: scope_key.to_owned(),
            source_kind: "user".to_owned(),
            created_at_unix: 1_700_000_000,
            updated_at_unix: 1_700_000_000,
        }
    }

    fn pack_skill_record(
        id: char,
        parent: &SkillPackInstallationRecord,
        member_key: &str,
    ) -> SkillInstallationRecord {
        SkillInstallationRecord {
            skill_id: SkillId::new(id.to_string().repeat(21)).expect("valid skill id"),
            owner: None,
            slug: format!("{member_key}-skill"),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: parent.scope_key.clone(),
            source_ref: format!("upload:{member_key}"),
            install_path: format!("/tmp/skills/{id}/{member_key}"),
            trust_level: "community".to_owned(),
            fingerprint: format!("fingerprint-{member_key}"),
            updated_at_unix: 1_700_000_000,
            pack_id: Some(parent.pack_id.clone()),
            pack_member_key: Some(member_key.to_owned()),
        }
    }

    #[test]
    fn atomic_skill_pack_write_sets_are_explicitly_bounded() {
        let parent = skill_pack_record('P', "bounded", "workspace-one");
        let child = pack_skill_record('C', &parent, "member");
        let oversized_children = vec![child; super::MAX_ATOMIC_SKILL_PACK_MEMBERS + 1];
        assert!(
            super::validate_skill_pack_children_scope(&parent, &oversized_children).is_err(),
            "oversized pack membership must be rejected before writer admission"
        );

        let audit = SkillAuditEventRecord {
            turn_id: None,
            skill_id: SkillId::new("A".repeat(21)).expect("valid skill id"),
            skill_owner: None,
            skill_slug: "bounded".to_owned(),
            source_kind: "user".to_owned(),
            action: "install".to_owned(),
            decision: "accepted".to_owned(),
            reason_code: None,
            details_json: "{}".to_owned(),
            created_at_unix: 1_700_000_000,
        };
        let oversized_audit = vec![audit; super::MAX_ATOMIC_SKILL_PACK_AUDIT_EVENTS + 1];
        assert!(
            super::validate_atomic_skill_audit_bound(&oversized_audit).is_err(),
            "oversized audit bundle must be rejected before writer admission"
        );
    }

    #[tokio::test]
    async fn skill_pack_child_transactions_preserve_membership_and_order() {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            let store = SkillPackRepository::new(db);
            let parent = skill_pack_record('P', "initial", "workspace-one");
            let zeta = pack_skill_record('Z', &parent, "zeta");
            let alpha = pack_skill_record('A', &parent, "alpha");
            store
                .insert_skill_pack_installation_with_children(
                    &parent,
                    &[zeta.clone(), alpha.clone()],
                )
                .await
                .expect("parent and children should insert atomically");

            let ordered = store
                .list_skill_installations_for_pack("workspace-one", &parent.pack_id)
                .await
                .expect("pack children should list");
            assert_eq!(
                ordered
                    .iter()
                    .map(|child| child.pack_member_key.as_deref())
                    .collect::<Vec<_>>(),
                vec![Some("alpha"), Some("zeta")]
            );
            assert!(
                store
                    .list_skill_installations_for_pack("workspace-two", &parent.pack_id)
                    .await
                    .expect("cross-scope child query should succeed")
                    .is_empty()
            );

            let mut retained = alpha.clone();
            retained.fingerprint = "updated-alpha".to_owned();
            retained.updated_at_unix = 1_700_000_100;
            let beta = pack_skill_record('B', &parent, "beta");
            let mut renamed_parent = parent.clone();
            renamed_parent.name = "renamed".to_owned();
            renamed_parent.updated_at_unix = 1_700_000_100;
            let diff = SkillPackChildDiff {
                retained: vec![retained.clone()],
                added: vec![beta.clone()],
                removed: vec![zeta.skill_id.clone()],
            };
            assert!(
                store
                    .update_skill_pack_installation_with_child_diff(&renamed_parent, &diff)
                    .await
                    .expect("pack diff should apply")
            );

            let updated = store
                .list_skill_installations_for_pack("workspace-one", &parent.pack_id)
                .await
                .expect("updated pack children should list");
            assert_eq!(updated.len(), 2);
            assert_eq!(updated[0].skill_id, alpha.skill_id);
            assert_eq!(updated[0].pack_id, Some(parent.pack_id.clone()));
            assert_eq!(updated[0].pack_member_key.as_deref(), Some("alpha"));
            assert_eq!(updated[0].fingerprint, "updated-alpha");
            assert_eq!(updated[1].skill_id, beta.skill_id);
            assert_eq!(
                store
                    .find_skill_pack_installation("workspace-one", &parent.pack_id)
                    .await
                    .expect("updated parent should query")
                    .expect("updated parent should exist")
                    .name,
                "renamed"
            );

            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn skill_pack_transactions_reject_cross_scope_and_delete_children_before_parent() {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            let store = SkillPackRepository::new(db);
            let parent = skill_pack_record('Q', "scoped", "workspace-one");
            let mut invalid_child = pack_skill_record('X', &parent, "member");
            invalid_child.scope_key = "workspace-two".to_owned();
            assert!(
                store
                    .insert_skill_pack_installation_with_children(&parent, &[invalid_child])
                    .await
                    .is_err()
            );
            assert!(
                store
                    .find_skill_pack_installation("workspace-one", &parent.pack_id)
                    .await
                    .expect("rolled-back parent lookup should succeed")
                    .is_none()
            );

            let child = pack_skill_record('Y', &parent, "member");
            store
                .insert_skill_pack_installation_with_children(&parent, &[child.clone()])
                .await
                .expect("valid pack should insert");
            let mut invalid_retained = child.clone();
            invalid_retained.scope_key = "workspace-two".to_owned();
            let mut attempted_parent = parent.clone();
            attempted_parent.name = "must-not-commit".to_owned();
            assert!(
                store
                    .update_skill_pack_installation_with_child_diff(
                        &attempted_parent,
                        &SkillPackChildDiff {
                            retained: vec![invalid_retained],
                            added: Vec::new(),
                            removed: Vec::new(),
                        },
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                store
                    .find_skill_pack_installation("workspace-one", &parent.pack_id)
                    .await
                    .expect("parent should query after rejected update")
                    .expect("parent should remain after rejected update")
                    .name,
                "scoped"
            );
            assert!(
                !store
                    .delete_skill_pack_installation_with_children("workspace-two", &parent.pack_id,)
                    .await
                    .expect("wrong-scope deletion should be a no-op")
            );
            assert!(
                skill_installation::find_skill_installation(db, &child.skill_id)
                    .await
                    .expect("child should remain after wrong-scope delete")
                    .is_some()
            );
            assert!(
                store
                    .delete_skill_pack_installation_with_children("workspace-one", &parent.pack_id,)
                    .await
                    .expect("pack removal should succeed")
            );
            assert!(
                skill_installation::find_skill_installation(db, &child.skill_id)
                    .await
                    .expect("removed child lookup should succeed")
                    .is_none()
            );
            assert!(
                store
                    .find_skill_pack_installation("workspace-one", &parent.pack_id)
                    .await
                    .expect("removed parent lookup should succeed")
                    .is_none()
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
