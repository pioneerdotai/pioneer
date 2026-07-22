use super::pack::{SkillPackMemberCandidate, prepare_skill_pack, reprepare_skill_pack_members};
use super::*;
use anyhow::{Result, anyhow, bail};
use std::collections::HashSet;
use std::path::Path;

fn generated_pack_id() -> Result<SkillPackId> {
    SkillPackId::new(pioneer_protocol::generate_id(
        pioneer_protocol::SKILL_PACK_ID_LEN,
    ))
    .map_err(|error| anyhow!("generated an invalid skill pack identity: {error}"))
}

pub(super) fn generated_child_id(upload_id: &str) -> Result<SkillId> {
    loop {
        let candidate = SkillId::new(pioneer_protocol::generate_id(
            pioneer_protocol::SKILL_ID_LEN,
        ))
        .map_err(|error| anyhow!("generated an invalid skill identity: {error}"))?;
        if candidate.as_str() != upload_id {
            return Ok(candidate);
        }
    }
}

pub(super) fn rollback_pack_installs(
    request_id: &str,
    committed: &[(String, pioneer_skills::InstallSkillResult)],
    lock_path: &Path,
) -> Result<()> {
    let mut failures = Vec::new();
    for (index, (member_key, result)) in committed.iter().enumerate().rev() {
        if let Err(error) = pack_install_failpoint(request_id, "rollback", index) {
            failures.push(format!("{member_key}: {error:#}"));
            continue;
        }
        match pioneer_skills::rollback_prepared_skill_commit(result, lock_path) {
            Ok(()) => {
                if let Some(parent) = result.install_path.parent()
                    && let Err(error) = std::fs::remove_dir(parent)
                    && error.kind() != std::io::ErrorKind::NotFound
                    && error.kind() != std::io::ErrorKind::DirectoryNotEmpty
                {
                    failures.push(format!(
                        "{member_key}: failed to remove empty install parent `{}`: {error}",
                        parent.display()
                    ));
                }
            }
            Err(error) => failures.push(format!("{member_key}: {error:#}")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("pack install rollback failed: {}", failures.join("; "))
    }
}

#[cfg(not(test))]
fn pack_install_failpoint(_request_id: &str, _stage: &str, _index: usize) -> Result<()> {
    Ok(())
}

#[cfg(test)]
static PACK_INSTALL_FAILPOINTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Vec<(&'static str, usize)>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn pack_install_failpoint(request_id: &str, stage: &str, index: usize) -> Result<()> {
    let failpoints = PACK_INSTALL_FAILPOINTS
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut failpoints = failpoints.lock().expect("pack install failpoint lock");
    let Some(points) = failpoints.get_mut(request_id) else {
        return Ok(());
    };
    let Some(position) = points
        .iter()
        .position(|(candidate_stage, candidate_index)| {
            *candidate_stage == stage && *candidate_index == index
        })
    else {
        return Ok(());
    };
    points.remove(position);
    if points.is_empty() {
        failpoints.remove(request_id);
    }
    bail!("injected pack install failure at {stage}[{index}]")
}

pub(super) fn with_pack_rollback_error(
    primary: anyhow::Error,
    rollback: Result<()>,
) -> anyhow::Error {
    match rollback {
        Ok(()) => primary,
        Err(rollback) => anyhow!("{primary:#}; rollback also failed: {rollback:#}"),
    }
}

fn pack_installation_record(
    pack_id: SkillPackId,
    pack_name: String,
    workspace_id: String,
    source_kind: SkillSourceKind,
    now: i64,
) -> SkillPackInstallationRecord {
    SkillPackInstallationRecord {
        pack_id,
        name: pack_name,
        scope_key: workspace_id,
        source_kind: source_kind.as_db_value().to_owned(),
        created_at_unix: now,
        updated_at_unix: now,
    }
}

pub(super) fn pack_child_installation_record(
    pack_id: &SkillPackId,
    member_key: &str,
    workspace_id: &str,
    source_ref: &str,
    source_kind: &SkillSourceKind,
    result: &pioneer_skills::InstallSkillResult,
    now: i64,
) -> SkillInstallationRecord {
    SkillInstallationRecord {
        skill_id: result.definition.identity.skill_id.clone(),
        owner: result.definition.identity.owner.clone(),
        slug: result.definition.identity.slug.clone(),
        version: result.definition.identity.version_hint.clone(),
        source_kind: source_kind.as_db_value().to_owned(),
        scope_key: workspace_id.to_owned(),
        source_ref: source_ref.to_owned(),
        install_path: result.install_path.display().to_string(),
        trust_level: trust_level_as_str(&result.definition.runtime.trust_level).to_owned(),
        fingerprint: result.definition.identity.fingerprint.clone(),
        updated_at_unix: now,
        pack_id: Some(pack_id.clone()),
        pack_member_key: Some(member_key.to_owned()),
    }
}

pub(super) fn lifecycle_result(record: &SkillInstallationRecord) -> SkillLifecycleResultSkill {
    SkillLifecycleResultSkill {
        skill_id: record.skill_id.clone(),
        owner: record.owner.clone(),
        slug: record.slug.clone(),
        source_kind: record.source_kind.clone(),
        version: record.version.clone(),
        fingerprint: record.fingerprint.clone(),
        trust_level: record.trust_level.clone(),
        install_path: record.install_path.clone(),
    }
}

impl MessageProcessor {
    #[cfg(test)]
    pub(in crate::message) fn set_pack_install_failpoints(
        &self,
        request_id: &str,
        points: Vec<(&'static str, usize)>,
    ) {
        let failpoints = PACK_INSTALL_FAILPOINTS
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        failpoints
            .lock()
            .expect("pack install failpoint lock")
            .insert(request_id.to_owned(), points);
    }

    async fn allocate_pack_install_id(&self) -> Result<SkillPackId> {
        loop {
            let candidate = generated_pack_id()?;
            if self
                .crud_store
                .find_skill_pack_installation_by_id(&candidate)
                .await?
                .is_none()
            {
                return Ok(candidate);
            }
        }
    }

    async fn allocate_pack_member_candidates(
        &self,
        member_keys: &[String],
        upload_id: &str,
        context: &SkillsRuntimeContext,
    ) -> Result<Vec<SkillPackMemberCandidate>> {
        let mut allocated = HashSet::new();
        let mut candidates = Vec::with_capacity(member_keys.len());
        for member_key in member_keys {
            loop {
                let candidate = generated_child_id(upload_id)?;
                if allocated.contains(&candidate)
                    || context
                        .catalog_params
                        .bundled
                        .iter()
                        .any(|entry| entry.skill_id == candidate)
                    || self
                        .crud_store
                        .find_skill_installation(&candidate)
                        .await?
                        .is_some()
                {
                    continue;
                }
                allocated.insert(candidate.clone());
                candidates.push(SkillPackMemberCandidate {
                    pack_member_key: member_key.clone(),
                    skill_id: candidate,
                });
                break;
            }
        }
        Ok(candidates)
    }

    pub(super) async fn reallocate_pack_member_candidates(
        &self,
        candidates: &mut [SkillPackMemberCandidate],
        collision_keys: &[String],
        upload_id: &str,
        context: &SkillsRuntimeContext,
    ) -> Result<()> {
        let collision_keys = collision_keys
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let mut reserved = candidates
            .iter()
            .filter(|candidate| !collision_keys.contains(candidate.pack_member_key.as_str()))
            .map(|candidate| candidate.skill_id.clone())
            .collect::<HashSet<_>>();
        let mut replaced = HashSet::new();

        for candidate in candidates {
            if !collision_keys.contains(candidate.pack_member_key.as_str()) {
                continue;
            }
            loop {
                let replacement = generated_child_id(upload_id)?;
                if reserved.contains(&replacement)
                    || context
                        .catalog_params
                        .bundled
                        .iter()
                        .any(|entry| entry.skill_id == replacement)
                    || self
                        .crud_store
                        .find_skill_installation(&replacement)
                        .await?
                        .is_some()
                {
                    continue;
                }
                reserved.insert(replacement.clone());
                candidate.skill_id = replacement;
                replaced.insert(candidate.pack_member_key.clone());
                break;
            }
        }
        if replaced.len() != collision_keys.len() {
            bail!("collision references an unknown pack member");
        }
        Ok(())
    }

    async fn pack_install_collisions(
        &self,
        request_id: &str,
        pack_id: &SkillPackId,
        candidates: &[SkillPackMemberCandidate],
        context: &SkillsRuntimeContext,
    ) -> Result<(bool, Vec<String>)> {
        let pack_collision = self
            .crud_store
            .find_skill_pack_installation_by_id(pack_id)
            .await?
            .is_some()
            || pack_install_failpoint(request_id, "pack_collision", 0).is_err();
        let mut child_collisions = Vec::new();
        for candidate in candidates {
            let bundled_collision = context
                .catalog_params
                .bundled
                .iter()
                .any(|entry| entry.skill_id == candidate.skill_id);
            let persisted_collision = self
                .crud_store
                .find_skill_installation(&candidate.skill_id)
                .await?
                .is_some();
            if bundled_collision || persisted_collision {
                child_collisions.push(candidate.pack_member_key.clone());
            }
        }
        if child_collisions.is_empty()
            && pack_install_failpoint(request_id, "child_collision", 0).is_err()
            && let Some(candidate) = candidates.first()
        {
            child_collisions.push(candidate.pack_member_key.clone());
        }
        Ok((pack_collision, child_collisions))
    }

    pub(crate) fn skills_pack_install<'a>(
        &'a self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsPackInstallParams,
    ) -> MessageFuture<'a, ()> {
        Box::pin(async move {
            let workspace_id = match self
                .validate_skills_workspace(
                    connection_id,
                    request_id.clone(),
                    params.workspace_id,
                    methods::SKILLS_PACK_INSTALL,
                )
                .await
            {
                Ok(workspace_id) => workspace_id,
                Err(error) => {
                    self.send_error(connection_id, error).await;
                    return;
                }
            };
            let target_source_kind =
                match parse_installable_source_kind(params.target_source_kind.as_str()) {
                    Some(kind) => kind,
                    None => {
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_PARAMS_CODE,
                                SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                                "pack install target_source_kind must be `user` or `registry`",
                                json!({"target_source_kind": params.target_source_kind}),
                            ),
                        )
                        .await;
                        return;
                    }
                };
            let context = match self.skills_runtime_context(workspace_id.as_str()) {
                Ok(context) => context,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to resolve skills runtime context",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let upload_id = match parse_lifecycle_upload_id(params.source) {
                Ok(upload_id) => upload_id,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            SKILLS_ERROR_INVALID_REQUEST,
                            "invalid lifecycle source",
                            json!({"error": error}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let (materialized, validated) = match self
                .materialize_uploaded_skill_pack_source(
                    connection_id,
                    workspace_id.as_str(),
                    upload_id.as_str(),
                    &context,
                    &request_id,
                )
                .await
            {
                Ok(materialized) => materialized,
                Err(error) => {
                    self.send_error(connection_id, error).await;
                    return;
                }
            };
            let Some(location) = install_location_for_source_kind(&context, &target_source_kind)
            else {
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "validated pack install source has no managed location",
                        json!({"target_source_kind": target_source_kind.as_db_value()}),
                    ),
                )
                .await;
                return;
            };
            let member_keys = validated
                .members
                .iter()
                .map(|member| member.pack_member_key.clone())
                .collect::<Vec<_>>();
            let mut pack_id = match self.allocate_pack_install_id().await {
                Ok(pack_id) => pack_id,
                Err(error) => {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to allocate skill pack identity",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let mut candidates = match self
                .allocate_pack_member_candidates(
                    member_keys.as_slice(),
                    materialized.upload.upload_id.as_str(),
                    &context,
                )
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to allocate pack member identities",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let source_ref = format!("upload:{}", materialized.upload.upload_id);
            let mut prepared = match prepare_skill_pack(
                validated.clone(),
                candidates.clone(),
                target_source_kind.clone(),
                source_ref.clone(),
                installer_policy(&context),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let mapped = map_lifecycle_error(&error, methods::SKILLS_PACK_INSTALL);
                    let (message, details) =
                        lifecycle_error_payload(&error, &mapped, None, &context.validation_policy);
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            mapped.jsonrpc_code,
                            mapped.code,
                            message,
                            details,
                        ),
                    )
                    .await;
                    return;
                }
            };

            let (upload_guard, write_guard) = loop {
                let upload_guard = self
                    .acquire_skill_upload_lock(materialized.upload.upload_id.as_str())
                    .await;
                let write_guard = self.acquire_skills_write_lock().await;
                if let Err(error) = self
                    .revalidate_finalized_upload_locked(
                        connection_id,
                        workspace_id.as_str(),
                        materialized.upload.upload_id.as_str(),
                        &request_id,
                    )
                    .await
                {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    drop(write_guard);
                    drop(upload_guard);
                    self.send_error(connection_id, error).await;
                    return;
                }
                if let Err(error) = self
                    .ensure_skills_lock_v2_locked(
                        location.lock_path.as_path(),
                        target_source_kind.clone(),
                        workspace_id.as_str(),
                    )
                    .await
                {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to convert skills lock",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
                let collisions = self
                    .pack_install_collisions(
                        request_id.as_str(),
                        &pack_id,
                        candidates.as_slice(),
                        &context,
                    )
                    .await;
                let (pack_collision, child_collision_keys) = match collisions {
                    Ok(collisions) => collisions,
                    Err(error) => {
                        let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to verify pack identities",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                };
                if !pack_collision && child_collision_keys.is_empty() {
                    break (upload_guard, write_guard);
                }
                drop(write_guard);
                drop(upload_guard);
                if pack_collision {
                    pack_id = match self.allocate_pack_install_id().await {
                        Ok(pack_id) => pack_id,
                        Err(error) => {
                            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                            self.send_error(
                                connection_id,
                                skills_error(
                                    Some(request_id),
                                    INVALID_REQUEST_CODE,
                                    SKILLS_ERROR_INTERNAL,
                                    "failed to reallocate skill pack identity",
                                    json!({"error": format!("{error:#}")}),
                                ),
                            )
                            .await;
                            return;
                        }
                    };
                }
                if !child_collision_keys.is_empty() {
                    if let Err(error) = self
                        .reallocate_pack_member_candidates(
                            candidates.as_mut_slice(),
                            child_collision_keys.as_slice(),
                            materialized.upload.upload_id.as_str(),
                            &context,
                        )
                        .await
                    {
                        let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to reallocate pack member identities",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                    if let Err(error) = reprepare_skill_pack_members(
                        &mut prepared,
                        &validated,
                        candidates.as_slice(),
                        child_collision_keys.as_slice(),
                        target_source_kind.clone(),
                        source_ref.as_str(),
                        installer_policy(&context),
                    ) {
                        let mapped = map_lifecycle_error(&error, methods::SKILLS_PACK_INSTALL);
                        let (message, details) = lifecycle_error_payload(
                            &error,
                            &mapped,
                            None,
                            &context.validation_policy,
                        );
                        let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                mapped.jsonrpc_code,
                                mapped.code,
                                message,
                                details,
                            ),
                        )
                        .await;
                        return;
                    }
                }
            };

            let now = now_timestamp_secs();
            let mut committed = Vec::with_capacity(prepared.members.len());
            for (member_index, member) in prepared.members.into_iter().enumerate() {
                if let Err(error) =
                    pack_install_failpoint(request_id.as_str(), "commit", member_index)
                {
                    let error = with_pack_rollback_error(
                        error.context(format!(
                            "failed to commit pack member `{}`",
                            member.pack_member_key
                        )),
                        rollback_pack_installs(
                            request_id.as_str(),
                            committed.as_slice(),
                            location.lock_path.as_path(),
                        ),
                    );
                    drop(write_guard);
                    drop(upload_guard);
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to publish skill pack",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
                match pioneer_skills::commit_prepared_skill(
                    pioneer_skills::CommitPreparedSkillRequest {
                        operation: pioneer_skills::InstallOperation::Install,
                        prepared: member.prepared,
                        install_root: location.install_root.clone(),
                        lock_path: location.lock_path.clone(),
                        previous: None,
                        expected_previous_fingerprint: None,
                        now_unix: now,
                        policy: installer_policy(&context),
                    },
                ) {
                    Ok(result) => committed.push((member.pack_member_key, result)),
                    Err(error) => {
                        let error = with_pack_rollback_error(
                            error.context(format!(
                                "failed to commit pack member `{}`",
                                member.pack_member_key
                            )),
                            rollback_pack_installs(
                                request_id.as_str(),
                                committed.as_slice(),
                                location.lock_path.as_path(),
                            ),
                        );
                        drop(write_guard);
                        drop(upload_guard);
                        let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to publish skill pack",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                }
            }

            committed.sort_by(|left, right| {
                left.0.cmp(&right.0).then_with(|| {
                    left.1
                        .definition
                        .identity
                        .skill_id
                        .cmp(&right.1.definition.identity.skill_id)
                })
            });
            let parent = pack_installation_record(
                pack_id.clone(),
                prepared.pack_name,
                workspace_id.clone(),
                target_source_kind.clone(),
                now,
            );
            let children = committed
                .iter()
                .map(|(member_key, result)| {
                    pack_child_installation_record(
                        &pack_id,
                        member_key.as_str(),
                        workspace_id.as_str(),
                        source_ref.as_str(),
                        &target_source_kind,
                        result,
                        now,
                    )
                })
                .collect::<Vec<_>>();
            let policies = children
                .iter()
                .map(|child| WorkspaceSkillPolicyRecord {
                    id: pioneer_protocol::generate_id(21),
                    workspace_id: workspace_id.clone(),
                    skill_id: child.skill_id.clone(),
                    enabled: Some(true),
                    allow_implicit_invocation: Some(false),
                })
                .collect::<Vec<_>>();
            let audit_records = committed
                .iter()
                .flat_map(|(_, result)| skill_audit_records(result.audit_events.as_slice()))
                .collect::<Vec<_>>();
            let persisted = match pack_install_failpoint(request_id.as_str(), "persist", 0) {
                Ok(()) => {
                    self.crud_store
                        .install_skill_pack_lifecycle(
                            &parent,
                            children.as_slice(),
                            policies.as_slice(),
                            audit_records.as_slice(),
                            materialized.upload.upload_id.as_str(),
                            now,
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            let persistence_error = match persisted {
                Ok(true) => None,
                Ok(false) => Some(anyhow!(
                    "upload `{}` changed state before pack publication",
                    materialized.upload.upload_id
                )),
                Err(error) => Some(error),
            };
            if let Some(primary) = persistence_error {
                let error = with_pack_rollback_error(
                    primary,
                    rollback_pack_installs(
                        request_id.as_str(),
                        committed.as_slice(),
                        location.lock_path.as_path(),
                    ),
                );
                drop(write_guard);
                drop(upload_guard);
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to persist skill pack installation",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }

            for (_, result) in &committed {
                pioneer_skills::finalize_prepared_skill_commit(result);
            }
            self.cleanup_upload_artifacts(
                &materialized.upload,
                materialized.cleanup_root.as_path(),
            );
            drop(write_guard);
            drop(upload_guard);

            let pack_item = SkillPackInstallationItem {
                id: parent.pack_id.clone(),
                name: parent.name.clone(),
                source_kind: parent.source_kind.clone(),
                created_at: parent.created_at_unix,
                updated_at: parent.updated_at_unix,
            };
            let response_skills = children.iter().map(lifecycle_result).collect::<Vec<_>>();
            let child_changes = children
                .iter()
                .map(|child| SkillChangedItem {
                    skill_id: child.skill_id.clone(),
                    owner: child.owner.clone(),
                    slug: child.slug.clone(),
                    source_kind: child.source_kind.clone(),
                    change_type: "install".to_owned(),
                    fingerprint_before: None,
                    fingerprint_after: Some(child.fingerprint.clone()),
                })
                .collect::<Vec<_>>();
            let payload = SkillsPackInstallResponse {
                status: "installed".to_owned(),
                pack: pack_item,
                skills: response_skills,
                audit: SkillLifecycleAuditSummary {
                    events_written: audit_records.len(),
                },
            };
            match JsonRpcResponse::from_result(request_id, &payload) {
                Ok(response) => {
                    if let Err(error) = self.send_json(connection_id, &response).await {
                        warn!(
                            connection_id,
                            error = %format!("{error:#}"),
                            "failed to send skills/pack/install response"
                        );
                    }
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            None,
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to encode skills/pack/install response",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                }
            }
            self.notify_skill_projection_changed(
                workspace_id.as_str(),
                "pack_installed",
                child_changes,
                vec![SkillPackChangedItem {
                    pack_id,
                    change_type: "installed".to_owned(),
                    name_before: None,
                    name_after: Some(parent.name),
                }],
                now,
            )
            .await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{generated_child_id, generated_pack_id, with_pack_rollback_error};

    #[test]
    fn generated_pack_and_child_ids_are_typed_and_distinct_from_upload() {
        let pack_id = generated_pack_id().expect("pack id");
        let child_id = generated_child_id(pack_id.as_str()).expect("child id");
        assert_eq!(pack_id.as_str().len(), pioneer_protocol::SKILL_PACK_ID_LEN);
        assert_eq!(child_id.as_str().len(), pioneer_protocol::SKILL_ID_LEN);
        assert_ne!(child_id.as_str(), pack_id.as_str());
    }

    #[test]
    fn rollback_failure_is_reported_as_compound_error() {
        let error = with_pack_rollback_error(
            anyhow::anyhow!("primary failure"),
            Err(anyhow::anyhow!("rollback failure")),
        )
        .to_string();
        assert!(error.contains("primary failure"));
        assert!(error.contains("rollback also failed"));
        assert!(error.contains("rollback failure"));
    }
}
