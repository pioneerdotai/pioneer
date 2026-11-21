//! Atomic skill publication and removal.

use anyhow::{Context, Result, bail};
use pioneer_protocol::{SkillId, SkillPackId};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, TransactionTrait};

use crate::plugins::{PluginNativeWrite, PluginOwnershipWrite};
use crate::skill_audit_event::SkillAuditEventRecord;
use crate::skill_pack_installation::{skill_pack_installation_record_from_model, unix_to_datetime};
use crate::skill_pack_transactions::{
    retry_write, skill_installation_record_from_model, validate_atomic_skill_audit_bound,
    validate_skill_pack_child,
};
use crate::skill_records::{
    SkillInstallationPatch, SkillInstallationRecord, WorkspaceSkillPolicyRecord,
};
use crate::{
    plugins, skill_audit_event, skill_installation, skill_pack_installation, skill_upload_session,
    skill_workspace_policy,
};

pub async fn insert_with_policy(
    db: &SqliteDatabase,
    installation: &SkillInstallationRecord,
    policy: &WorkspaceSkillPolicyRecord,
    event_timestamp_secs: i64,
) -> Result<()> {
    if installation.pack_id.is_some() || installation.pack_member_key.is_some() {
        bail!("pack members must be inserted through a pack transaction");
    }
    let now = unix_to_datetime(event_timestamp_secs);
    let prepared_installation =
        skill_installation::prepare_skill_installation(installation, now, now);
    let prepared_policy = skill_workspace_policy::prepare_workspace_skill_policy(policy, now, now);
    retry_write(|| {
        let prepared_installation = prepared_installation.clone();
        let prepared_policy = prepared_policy.clone();
        async move {
            let transaction = db
                .begin()
                .await
                .context("failed to begin skill installation transaction")?;
            if let Err(error) = skill_installation::insert_prepared_skill_installations(
                &transaction,
                std::slice::from_ref(&prepared_installation),
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            if let Err(error) = skill_workspace_policy::upsert_prepared_workspace_skill_policies(
                &transaction,
                std::slice::from_ref(&prepared_policy),
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            transaction
                .commit()
                .await
                .context("failed to commit skill installation transaction")
        }
    })
    .await
}

pub async fn install(
    db: &SqliteDatabase,
    installation: &SkillInstallationRecord,
    policy: &WorkspaceSkillPolicyRecord,
    audit_records: &[SkillAuditEventRecord],
    upload_id: Option<&str>,
    ownership: Option<&PluginOwnershipWrite>,
    event_timestamp_secs: i64,
) -> Result<bool> {
    if installation.pack_id.is_some() || installation.pack_member_key.is_some() {
        bail!("pack members must be inserted through a pack transaction");
    }
    if policy.workspace_id != installation.scope_key || policy.skill_id != installation.skill_id {
        bail!("skill install lifecycle requires the matching workspace policy");
    }
    if audit_records.is_empty()
        || audit_records
            .iter()
            .any(|audit| audit.skill_id.as_str() != installation.skill_id.as_str())
    {
        bail!("skill install lifecycle requires audit events for the installed SkillId");
    }
    validate_atomic_skill_audit_bound(audit_records)?;

    if upload_id.is_some() == ownership.is_some() {
        bail!("skill publication requires either finalized upload or package ownership");
    }
    let prepared_link = ownership.map(|write| plugins::prepare_link(write, "skill"));
    let now = unix_to_datetime(event_timestamp_secs);
    let prepared_installation =
        skill_installation::prepare_skill_installation(installation, now, now);
    let prepared_policy = skill_workspace_policy::prepare_workspace_skill_policy(policy, now, now);
    let prepared_audit_records = skill_audit_event::prepare_skill_audit_events(None, audit_records);
    let upload_id = upload_id.map(str::to_owned);
    retry_write(|| {
        let prepared_installation = prepared_installation.clone();
        let prepared_policy = prepared_policy.clone();
        let prepared_audit_records = prepared_audit_records.clone();
        let upload_id = upload_id.clone();
        let prepared_link = prepared_link.clone();
        async move {
            let transaction = db
                .begin()
                .await
                .context("failed to begin skill lifecycle install transaction")?;
            let result: Result<bool> = async {
                plugins::validate_publication(
                    &transaction,
                    ownership,
                    &installation.scope_key,
                    "skill",
                    installation.skill_id.as_str(),
                    None,
                )
                .await?;
                skill_installation::insert_prepared_skill_installations(
                    &transaction,
                    std::slice::from_ref(&prepared_installation),
                )
                .await?;
                skill_workspace_policy::upsert_prepared_workspace_skill_policies(
                    &transaction,
                    std::slice::from_ref(&prepared_policy),
                )
                .await?;
                skill_audit_event::insert_prepared_skill_audit_events(
                    &transaction,
                    prepared_audit_records,
                )
                .await?;
                if let Some(link) = prepared_link {
                    plugins::publish(&transaction, link).await?;
                }
                if let Some(upload_id) = upload_id {
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
                }
                Ok(true)
            }
            .await;
            match result {
                Ok(true) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit skill lifecycle install")?;
                    Ok(true)
                }
                Ok(false) => {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back conflicted skill lifecycle install")?;
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

pub async fn uninstall(
    db: &SqliteDatabase,
    expected: &SkillInstallationRecord,
    audit_records: &[SkillAuditEventRecord],
    native: Option<&PluginNativeWrite>,
    event_timestamp_secs: i64,
) -> Result<bool> {
    plugins::validate_native_write_input(native)?;
    let package_removal = plugins::native_package_removal(native)?;
    if audit_records.is_empty()
        || audit_records
            .iter()
            .any(|audit| audit.skill_id != expected.skill_id)
    {
        bail!("skill uninstall requires audit events for the removed SkillId");
    }
    validate_atomic_skill_audit_bound(audit_records)?;
    let Some(expected_model) =
        skill_installation::find_skill_installation(db, &expected.skill_id).await?
    else {
        return Ok(false);
    };
    if skill_installation_record_from_model(expected_model.clone())? != *expected {
        return Ok(false);
    }
    let expected_parent_model = if let Some(pack_id) = expected.pack_id.as_ref() {
        let parent_model = skill_pack_installation::find_skill_pack_installation(
            db,
            expected.scope_key.as_str(),
            pack_id,
        )
        .await?
        .with_context(|| {
            format!(
                "pack `{pack_id}` for skill `{}` is missing",
                expected.skill_id
            )
        })?;
        let parent = skill_pack_installation_record_from_model(parent_model.clone())?;
        validate_skill_pack_child(&parent, expected)?;
        Some(parent_model)
    } else {
        None
    };
    let now = unix_to_datetime(event_timestamp_secs);
    let expected = expected.clone();
    let prepared_audit_records = skill_audit_event::prepare_skill_audit_events(None, audit_records);
    retry_write(|| {
        let expected = expected.clone();
        let prepared_audit_records = prepared_audit_records.clone();
        let expected_model = expected_model.clone();
        let expected_parent_model = expected_parent_model.clone();
        async move {
            let transaction = db
                .begin()
                .await
                .context("failed to begin skill lifecycle uninstall transaction")?;
            let result: Result<bool> = async {
                if let Some(pack_id) = expected.pack_id.as_ref() {
                    let Some(parent_model) = skill_pack_installation::lock_skill_pack_installation(
                        &transaction,
                        expected.scope_key.as_str(),
                        pack_id,
                    )
                    .await?
                    else {
                        return Ok(false);
                    };
                    if expected_parent_model.as_ref() != Some(&parent_model) {
                        return Ok(false);
                    }
                }

                let Some(current_model) =
                    skill_installation::lock_skill_installation(&transaction, &expected.skill_id)
                        .await?
                else {
                    return Ok(false);
                };
                if current_model != expected_model {
                    return Ok(false);
                }

                plugins::validate_native_write(
                    &transaction,
                    native,
                    &expected.scope_key,
                    "skill",
                    expected.skill_id.as_str(),
                )
                .await?;

                skill_workspace_policy::delete_workspace_skill_policy(
                    &transaction,
                    expected.scope_key.as_str(),
                    &expected.skill_id,
                )
                .await?;
                if !skill_installation::delete_skill_installation(&transaction, &expected.skill_id)
                    .await?
                {
                    return Ok(false);
                }
                if let Some(parent_model) = expected_parent_model.as_ref()
                    && !skill_pack_installation::update_skill_pack_installation_name(
                        &transaction,
                        parent_model.scope_key.as_str(),
                        expected
                            .pack_id
                            .as_ref()
                            .context("pack child lost its pack id during uninstall")?,
                        parent_model.name.as_str(),
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
                if let Some(native) = native {
                    plugins::publish_native_write(&transaction, native, "skill", true).await?;
                    if package_removal {
                        plugins::mark_package_removed(&transaction, native, "skill").await?;
                    }
                }
                Ok(true)
            }
            .await;
            match result {
                Ok(true) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit skill lifecycle uninstall")?;
                    Ok(true)
                }
                Ok(false) => {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back conflicted skill uninstall")?;
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

#[derive(Clone)]
pub struct GenericSkillUpdateFence {
    existing: Option<pioneer_entity::skill_installation::Model>,
    parent: Option<pioneer_entity::skill_pack_installation::Model>,
    parent_scope_key: Option<String>,
    parent_pack_id: Option<SkillPackId>,
}

pub async fn prepare_generic_skill_update_fence<C: ConnectionTrait>(
    connection: &C,
    skill_id: &SkillId,
    patch: &SkillInstallationPatch,
) -> Result<GenericSkillUpdateFence> {
    let Some(existing) = skill_installation::find_skill_installation(connection, skill_id).await?
    else {
        return Ok(GenericSkillUpdateFence {
            existing: None,
            parent: None,
            parent_scope_key: None,
            parent_pack_id: None,
        });
    };
    let existing_record = skill_installation_record_from_model(existing.clone())?;
    let Some(pack_id) = existing_record.pack_id.as_ref() else {
        return Ok(GenericSkillUpdateFence {
            existing: Some(existing),
            parent: None,
            parent_scope_key: None,
            parent_pack_id: None,
        });
    };
    let parent = skill_pack_installation::find_skill_pack_installation(
        connection,
        existing_record.scope_key.as_str(),
        pack_id,
    )
    .await?
    .with_context(|| {
        format!(
            "pack `{pack_id}` for skill `{skill_id}` is missing from scope `{}`",
            existing_record.scope_key
        )
    })?;
    let parent_record = skill_pack_installation_record_from_model(parent.clone())?;
    validate_skill_pack_child(&parent_record, &existing_record)?;
    if patch
        .scope_key
        .as_ref()
        .is_some_and(|scope_key| scope_key != &existing_record.scope_key)
    {
        bail!("generic skill updates cannot move a pack child across scopes");
    }
    if patch
        .source_kind
        .as_ref()
        .is_some_and(|source_kind| source_kind != &parent_record.source_kind)
    {
        bail!("generic skill updates cannot change a pack child's source kind");
    }
    Ok(GenericSkillUpdateFence {
        existing: Some(existing),
        parent: Some(parent),
        parent_scope_key: Some(existing_record.scope_key),
        parent_pack_id: Some(pack_id.clone()),
    })
}

pub async fn revalidate_generic_skill_update_fence<C: ConnectionTrait>(
    connection: &C,
    skill_id: &SkillId,
    fence: &GenericSkillUpdateFence,
) -> Result<bool> {
    let parent = match (&fence.parent_scope_key, &fence.parent_pack_id) {
        (Some(scope_key), Some(pack_id)) => {
            skill_pack_installation::lock_skill_pack_installation(
                connection,
                scope_key.as_str(),
                pack_id,
            )
            .await?
        }
        (None, None) => None,
        _ => bail!("generic skill update fence has incomplete parent identity"),
    };
    if parent != fence.parent {
        return Ok(false);
    }
    let existing = skill_installation::lock_skill_installation(connection, skill_id).await?;
    Ok(existing == fence.existing)
}

pub async fn update(
    db: &SqliteDatabase,
    skill_id: &SkillId,
    patch: &SkillInstallationPatch,
    audit_records: &[SkillAuditEventRecord],
    upload_id: Option<&str>,
    ownership: Option<&PluginOwnershipWrite>,
    native: Option<&PluginNativeWrite>,
    event_timestamp_secs: i64,
) -> Result<bool> {
    if audit_records.is_empty()
        || audit_records
            .iter()
            .any(|audit| &audit.skill_id != skill_id)
    {
        bail!("skill update lifecycle requires audit events for the updated SkillId");
    }
    validate_atomic_skill_audit_bound(audit_records)?;
    if native.is_some() && (ownership.is_some() || upload_id.is_none()) {
        bail!("native skill edit requires its finalized upload");
    }
    plugins::validate_native_write_input(native)?;

    if upload_id.is_some() == ownership.is_some() {
        bail!("skill update requires either finalized upload or package ownership");
    }
    let prepared_link = ownership.map(|write| plugins::prepare_link(write, "skill"));
    let fence = prepare_generic_skill_update_fence(db, skill_id, patch).await?;
    let now = unix_to_datetime(event_timestamp_secs);
    let skill_id = skill_id.clone();
    let patch = patch.clone();
    let prepared_audit_records = skill_audit_event::prepare_skill_audit_events(None, audit_records);
    let upload_id = upload_id.map(str::to_owned);
    retry_write(|| {
        let skill_id = skill_id.clone();
        let patch = patch.clone();
        let prepared_audit_records = prepared_audit_records.clone();
        let upload_id = upload_id.clone();
        let prepared_link = prepared_link.clone();
        let fence = fence.clone();
        async move {
            let transaction = db
                .begin()
                .await
                .context("failed to begin skill lifecycle update transaction")?;
            let result: Result<bool> = async {
                if !revalidate_generic_skill_update_fence(&transaction, &skill_id, &fence).await? {
                    return Ok(false);
                }
                let Some(existing) = fence.existing.as_ref() else {
                    return Ok(false);
                };
                if ownership.is_some()
                    && (patch
                        .scope_key
                        .as_ref()
                        .is_some_and(|scope| scope != &existing.scope_key)
                        || existing.pack_id.is_some())
                {
                    bail!("plugin child cannot move scope or belong to a pack");
                }
                if native.is_some() {
                    plugins::validate_native_write(
                        &transaction,
                        native,
                        &existing.scope_key,
                        "skill",
                        skill_id.as_str(),
                    )
                    .await?;
                } else {
                    plugins::validate_publication(
                        &transaction,
                        ownership,
                        &existing.scope_key,
                        "skill",
                        skill_id.as_str(),
                        Some(skill_id.as_str()),
                    )
                    .await?;
                }

                if !skill_installation::update_skill_installation(
                    &transaction,
                    &skill_id,
                    &patch,
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
                if let Some(link) = prepared_link {
                    plugins::publish(&transaction, link).await?;
                }
                if let Some(write) = native {
                    plugins::publish_native_write(&transaction, write, "skill", false).await?;
                }
                if let Some(upload_id) = upload_id {
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
                }
                Ok(true)
            }
            .await;
            match result {
                Ok(true) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit skill lifecycle update")?;
                    Ok(true)
                }
                Ok(false) => {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back conflicted skill lifecycle update")?;
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

pub async fn delete_with_workspace_policy(
    db: &SqliteDatabase,
    workspace_id: &str,
    skill_id: &SkillId,
) -> Result<bool> {
    retry_write(|| async {
        let transaction = db
            .begin()
            .await
            .context("failed to begin skill uninstall transaction")?;
        let removed =
            match skill_installation::delete_skill_installation(&transaction, skill_id).await {
                Ok(removed) => removed,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
        if !removed {
            let _ = transaction.rollback().await;
            return Ok(false);
        }
        if let Err(error) = skill_workspace_policy::delete_workspace_skill_policy(
            &transaction,
            workspace_id,
            skill_id,
        )
        .await
        {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        transaction
            .commit()
            .await
            .context("failed to commit skill uninstall transaction")?;
        Ok(true)
    })
    .await
}
