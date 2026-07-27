use super::pack::{SkillPackMemberCandidate, prepare_skill_pack, reprepare_skill_pack_members};
use super::pack_install::{
    generated_child_id, lifecycle_result, pack_child_installation_record, rollback_pack_installs,
    with_pack_rollback_error,
};
use super::*;
use anyhow::{Context, Result, anyhow, bail};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackMemberDiff {
    retained: Vec<SkillInstallationRecord>,
    added_keys: Vec<String>,
    removed: Vec<SkillInstallationRecord>,
}

fn build_pack_member_diff(
    current: &[SkillInstallationRecord],
    next_member_keys: &[String],
) -> Result<PackMemberDiff> {
    let mut current_by_key = BTreeMap::new();
    for child in current {
        let key = child
            .pack_member_key
            .clone()
            .with_context(|| format!("pack child `{}` has no member key", child.skill_id))?;
        if current_by_key.insert(key.clone(), child.clone()).is_some() {
            bail!("pack contains duplicate current member key `{key}`");
        }
    }

    let mut retained = Vec::new();
    let mut added_keys = Vec::new();
    let mut seen_next = HashSet::new();
    for key in next_member_keys {
        if !seen_next.insert(key.clone()) {
            bail!("updated pack contains duplicate member key `{key}`");
        }
        if let Some(current) = current_by_key.remove(key) {
            retained.push(current);
        } else {
            added_keys.push(key.clone());
        }
    }
    let removed = current_by_key.into_values().collect::<Vec<_>>();
    Ok(PackMemberDiff {
        retained,
        added_keys,
        removed,
    })
}

fn rollback_pack_update(
    request_id: &str,
    removals: Option<&pioneer_skills::ReversibleSkillRemovalBatch>,
    committed: &[(String, pioneer_skills::InstallSkillResult)],
    lock_path: &Path,
) -> Result<()> {
    let mut failures = Vec::new();
    if let Some(removals) = removals {
        if let Err(error) = pack_update_failpoint(request_id, "rollback_removals", 0) {
            failures.push(format!("removed children: {error:#}"));
        } else if let Err(error) = pioneer_skills::rollback_reversible_skill_removals(removals) {
            failures.push(format!("removed children: {error:#}"));
        }
    }
    if let Err(error) = pack_update_failpoint(request_id, "rollback_commits", 0) {
        failures.push(format!("retained/added children: {error:#}"));
    } else if let Err(error) = rollback_pack_installs(request_id, committed, lock_path) {
        failures.push(format!("retained/added children: {error:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("pack update rollback failed: {}", failures.join("; "))
    }
}

#[cfg(not(test))]
fn pack_update_failpoint(_request_id: &str, _stage: &str, _index: usize) -> Result<()> {
    Ok(())
}

#[cfg(test)]
static PACK_UPDATE_FAILPOINTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Vec<(&'static str, usize)>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn pack_update_failpoint(request_id: &str, stage: &str, index: usize) -> Result<()> {
    let failpoints = PACK_UPDATE_FAILPOINTS
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut failpoints = failpoints.lock().expect("pack update failpoint lock");
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
    bail!("injected pack update failure at {stage}[{index}]")
}

impl MessageProcessor {
    #[cfg(test)]
    pub(in crate::message) fn set_pack_update_failpoints(
        &self,
        request_id: &str,
        points: Vec<(&'static str, usize)>,
    ) {
        let failpoints = PACK_UPDATE_FAILPOINTS
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        failpoints
            .lock()
            .expect("pack update failpoint lock")
            .insert(request_id.to_owned(), points);
    }

    async fn allocate_pack_update_candidates(
        &self,
        member_keys: &[String],
        diff: &PackMemberDiff,
        upload_id: &str,
        context: &SkillsRuntimeContext,
    ) -> Result<Vec<SkillPackMemberCandidate>> {
        let retained_by_key = diff
            .retained
            .iter()
            .map(|child| {
                (
                    child
                        .pack_member_key
                        .clone()
                        .expect("validated retained child must have member key"),
                    child.skill_id.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut reserved = diff
            .retained
            .iter()
            .chain(diff.removed.iter())
            .map(|child| child.skill_id.clone())
            .collect::<HashSet<_>>();
        let mut candidates = Vec::with_capacity(member_keys.len());
        for member_key in member_keys {
            if let Some(skill_id) = retained_by_key.get(member_key) {
                candidates.push(SkillPackMemberCandidate {
                    pack_member_key: member_key.clone(),
                    skill_id: skill_id.clone(),
                });
                continue;
            }
            loop {
                let candidate = generated_child_id(upload_id)?;
                if reserved.contains(&candidate)
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
                reserved.insert(candidate.clone());
                candidates.push(SkillPackMemberCandidate {
                    pack_member_key: member_key.clone(),
                    skill_id: candidate,
                });
                break;
            }
        }
        Ok(candidates)
    }

    async fn added_pack_update_collisions(
        &self,
        request_id: &str,
        candidates: &[SkillPackMemberCandidate],
        added_keys: &HashSet<String>,
        context: &SkillsRuntimeContext,
    ) -> Result<Vec<String>> {
        let mut collisions = Vec::new();
        for candidate in candidates {
            if !added_keys.contains(&candidate.pack_member_key) {
                continue;
            }
            if context
                .catalog_params
                .bundled
                .iter()
                .any(|entry| entry.skill_id == candidate.skill_id)
                || self
                    .crud_store
                    .find_skill_installation(&candidate.skill_id)
                    .await?
                    .is_some()
            {
                collisions.push(candidate.pack_member_key.clone());
            }
        }
        if collisions.is_empty()
            && pack_update_failpoint(request_id, "collision", 0).is_err()
            && let Some(candidate) = candidates
                .iter()
                .find(|candidate| added_keys.contains(&candidate.pack_member_key))
        {
            collisions.push(candidate.pack_member_key.clone());
        }
        Ok(collisions)
    }

    pub(crate) fn skills_pack_update<'a>(
        &'a self,
        request_context: &'a RequestContext,
        request_id: RequestId,
        params: SkillsPackUpdateParams,
    ) -> MessageFuture<'a, ()> {
        let connection_id = request_context.connection_id();
        Box::pin(async move {
            let workspace_id = match self
                .validate_skills_workspace(
                    connection_id,
                    request_id.clone(),
                    params.workspace_id,
                    methods::SKILLS_PACK_UPDATE,
                )
                .await
            {
                Ok(workspace_id) => workspace_id,
                Err(error) => {
                    self.send_error(connection_id, error).await;
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
            let expected_parent = match self
                .crud_store
                .find_skill_pack_installation(workspace_id.as_str(), &params.pack_id)
                .await
            {
                Ok(Some(parent)) => parent,
                Ok(None) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_NOT_FOUND,
                            "skill pack installation was not found",
                            json!({"pack_id": params.pack_id}),
                        ),
                    )
                    .await;
                    return;
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to read skill pack installation",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let expected_children = match self
                .crud_store
                .list_skill_installations_for_pack(workspace_id.as_str(), &params.pack_id)
                .await
            {
                Ok(children) => children,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to read skill pack children",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let (source_kind, location) = match install_location_for_stored_source_kind(
                &context,
                expected_parent.source_kind.as_str(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "stored skill pack has an invalid lifecycle source",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let expected_path_presence = expected_children
                .iter()
                .map(|child| {
                    (
                        child.skill_id.clone(),
                        Path::new(child.install_path.as_str()).exists(),
                    )
                })
                .collect::<Vec<_>>();
            let expected_lock = if location.lock_path.exists() {
                match pioneer_skills::read_skills_lock(location.lock_path.as_path()) {
                    Ok(lock) => Some(lock),
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to snapshot skills lock before pack update",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                }
            } else {
                None
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
            let member_keys = validated
                .members
                .iter()
                .map(|member| member.pack_member_key.clone())
                .collect::<Vec<_>>();
            let diff = match build_pack_member_diff(&expected_children, member_keys.as_slice()) {
                Ok(diff) => diff,
                Err(error) => {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to build skill pack update diff",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let added_keys = diff.added_keys.iter().cloned().collect::<HashSet<_>>();
            let mut candidates = match self
                .allocate_pack_update_candidates(
                    member_keys.as_slice(),
                    &diff,
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
                            "failed to allocate added pack member identities",
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
                source_kind.clone(),
                source_ref.clone(),
                installer_policy(&context),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let mapped = map_lifecycle_error(&error, methods::SKILLS_PACK_UPDATE);
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
                        source_kind.clone(),
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
                let current_parent = self
                    .crud_store
                    .find_skill_pack_installation(workspace_id.as_str(), &expected_parent.pack_id)
                    .await;
                let current_children = self
                    .crud_store
                    .list_skill_installations_for_pack(
                        workspace_id.as_str(),
                        &expected_parent.pack_id,
                    )
                    .await;
                let current_lock = pioneer_skills::read_skills_lock(location.lock_path.as_path());
                let current_path_presence = expected_children
                    .iter()
                    .map(|child| {
                        (
                            child.skill_id.clone(),
                            Path::new(child.install_path.as_str()).exists(),
                        )
                    })
                    .collect::<Vec<_>>();
                let stale = pack_update_failpoint(request_id.as_str(), "stale", 0).is_err()
                    || !matches!(current_parent, Ok(Some(ref parent)) if parent == &expected_parent)
                    || !matches!(current_children, Ok(ref children) if children == &expected_children)
                    || expected_lock
                        .as_ref()
                        .is_some_and(|expected| current_lock.as_ref().ok() != Some(expected))
                    || current_path_presence != expected_path_presence;
                if stale {
                    drop(write_guard);
                    drop(upload_guard);
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_UPDATE_CONFLICT_FINGERPRINT,
                            "skill pack changed while update was prepared",
                            json!({"pack_id": expected_parent.pack_id}),
                        ),
                    )
                    .await;
                    return;
                }
                let collisions = self
                    .added_pack_update_collisions(
                        request_id.as_str(),
                        candidates.as_slice(),
                        &added_keys,
                        &context,
                    )
                    .await;
                match collisions {
                    Ok(collisions) if collisions.is_empty() => break (upload_guard, write_guard),
                    Ok(collisions) => {
                        drop(write_guard);
                        drop(upload_guard);
                        if let Err(error) = self
                            .reallocate_pack_member_candidates(
                                candidates.as_mut_slice(),
                                collisions.as_slice(),
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
                                    "failed to reallocate added pack member identities",
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
                            collisions.as_slice(),
                            source_kind.clone(),
                            source_ref.as_str(),
                            installer_policy(&context),
                        ) {
                            let mapped = map_lifecycle_error(&error, methods::SKILLS_PACK_UPDATE);
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
                    Err(error) => {
                        drop(write_guard);
                        drop(upload_guard);
                        let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to verify added pack member identities",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                }
            };

            let now = now_timestamp_secs();
            let old_by_key = expected_children
                .iter()
                .map(|child| {
                    (
                        child
                            .pack_member_key
                            .clone()
                            .expect("validated pack child member key"),
                        child,
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let mut committed = Vec::with_capacity(prepared.members.len());
            for (index, member) in prepared.members.into_iter().enumerate() {
                if let Err(error) = pack_update_failpoint(request_id.as_str(), "commit", index) {
                    let error = with_pack_rollback_error(
                        error.context(format!(
                            "failed to commit updated pack member `{}`",
                            member.pack_member_key
                        )),
                        rollback_pack_update(
                            request_id.as_str(),
                            None,
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
                            "failed to publish skill pack update",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
                let previous = old_by_key.get(member.pack_member_key.as_str()).copied();
                let operation = if previous.is_some() {
                    pioneer_skills::InstallOperation::Update
                } else {
                    pioneer_skills::InstallOperation::Install
                };
                let commit = pioneer_skills::commit_prepared_skill(
                    pioneer_skills::CommitPreparedSkillRequest {
                        operation,
                        prepared: member.prepared,
                        install_root: location.install_root.clone(),
                        lock_path: location.lock_path.clone(),
                        previous: previous.map(|child| pioneer_skills::PreviousSkillInstallation {
                            managed_install_path: pioneer_owned_install_path(
                                &location,
                                child.install_path.as_str(),
                            ),
                            fingerprint: child.fingerprint.clone(),
                        }),
                        expected_previous_fingerprint: previous
                            .map(|child| child.fingerprint.clone()),
                        now_unix: now,
                        policy: installer_policy(&context),
                    },
                );
                match commit {
                    Ok(result) => committed.push((member.pack_member_key, result)),
                    Err(error) => {
                        let error = with_pack_rollback_error(
                            error.context(format!(
                                "failed to commit updated pack member `{}`",
                                member.pack_member_key
                            )),
                            rollback_pack_update(
                                request_id.as_str(),
                                None,
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
                                "failed to publish skill pack update",
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

            let removal_targets = diff
                .removed
                .iter()
                .map(|child| {
                    let managed_install_path =
                        pioneer_owned_install_path(&location, child.install_path.as_str())
                            .with_context(|| {
                                format!(
                                    "removed pack child `{}` has an unmanaged install path",
                                    child.skill_id
                                )
                            })?;
                    Ok(pioneer_skills::ReversibleSkillRemovalTarget {
                        skill_id: child.skill_id.clone(),
                        slug: child.slug.clone(),
                        managed_install_path: Some(managed_install_path),
                    })
                })
                .collect::<Result<Vec<_>>>();
            let removal_targets = match removal_targets {
                Ok(targets) => targets,
                Err(error) => {
                    let error = with_pack_rollback_error(
                        error,
                        rollback_pack_update(
                            request_id.as_str(),
                            None,
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
                            "failed to stage removed pack members",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
            let removals = match pioneer_skills::stage_reversible_skill_removals(
                pioneer_skills::StageReversibleSkillRemovalsRequest {
                    targets: removal_targets,
                    install_root: location.install_root.clone(),
                    lock_path: location.lock_path.clone(),
                },
            ) {
                Ok(removals) => removals,
                Err(error) => {
                    let error = with_pack_rollback_error(
                        error,
                        rollback_pack_update(
                            request_id.as_str(),
                            None,
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
                            "failed to stage removed pack members",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };

            let next_parent = SkillPackInstallationRecord {
                pack_id: expected_parent.pack_id.clone(),
                name: prepared.pack_name,
                scope_key: expected_parent.scope_key.clone(),
                source_kind: expected_parent.source_kind.clone(),
                created_at_unix: expected_parent.created_at_unix,
                updated_at_unix: now,
            };
            let next_children = committed
                .iter()
                .map(|(member_key, result)| {
                    pack_child_installation_record(
                        &next_parent.pack_id,
                        member_key.as_str(),
                        workspace_id.as_str(),
                        source_ref.as_str(),
                        &source_kind,
                        result,
                        now,
                    )
                })
                .collect::<Vec<_>>();
            let retained_keys = diff
                .retained
                .iter()
                .map(|child| {
                    child
                        .pack_member_key
                        .clone()
                        .expect("validated retained member key")
                })
                .collect::<HashSet<_>>();
            let retained = next_children
                .iter()
                .filter(|child| {
                    child
                        .pack_member_key
                        .as_ref()
                        .is_some_and(|key| retained_keys.contains(key))
                })
                .cloned()
                .collect::<Vec<_>>();
            let added = next_children
                .iter()
                .filter(|child| {
                    child
                        .pack_member_key
                        .as_ref()
                        .is_some_and(|key| added_keys.contains(key))
                })
                .cloned()
                .collect::<Vec<_>>();
            let child_diff = pioneer_crud::SkillPackChildDiff {
                retained,
                added: added.clone(),
                removed: diff
                    .removed
                    .iter()
                    .map(|child| child.skill_id.clone())
                    .collect(),
            };
            let added_policies = added
                .iter()
                .map(|child| WorkspaceSkillPolicyRecord {
                    id: pioneer_protocol::generate_id(21),
                    workspace_id: workspace_id.clone(),
                    skill_id: child.skill_id.clone(),
                    enabled: Some(true),
                    allow_implicit_invocation: Some(false),
                })
                .collect::<Vec<_>>();
            let mut audit_records = committed
                .iter()
                .flat_map(|(_, result)| skill_audit_records(result.audit_events.as_slice()))
                .collect::<Vec<_>>();
            for removed in &diff.removed {
                let removal = removals
                    .removals
                    .iter()
                    .find(|removal| removal.skill_id == removed.skill_id)
                    .expect("staged removal must cover every removed child");
                audit_records.extend(skill_audit_records(&[
                    pioneer_skills::SkillAuditEvent::uninstall(
                        removed.skill_id.clone(),
                        removed.owner.clone(),
                        removed.slug.clone(),
                        removed.source_kind.clone(),
                        json!({
                            "install_path": removed.install_path,
                            "was_present_on_disk": removal.was_present_on_disk,
                            "had_lock_entry": removal.removed_lock_entry.is_some(),
                            "managed_path_removed": true,
                            "pack_id": next_parent.pack_id,
                            "pack_member_key": removed.pack_member_key,
                        }),
                        now,
                    ),
                ]));
            }

            let persisted = self
                .crud_store
                .update_skill_pack_lifecycle(
                    &expected_parent,
                    expected_children.as_slice(),
                    &next_parent,
                    &child_diff,
                    added_policies.as_slice(),
                    audit_records.as_slice(),
                    materialized.upload.upload_id.as_str(),
                    now,
                )
                .await;
            let persistence_error = match persisted {
                Ok(true) => None,
                Ok(false) => Some(anyhow!("skill pack changed before update publication")),
                Err(error) => Some(error),
            };
            if let Some(primary) = persistence_error {
                let error = with_pack_rollback_error(
                    primary,
                    rollback_pack_update(
                        request_id.as_str(),
                        Some(&removals),
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
                        "failed to persist skill pack update",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }

            for (_, result) in &committed {
                pioneer_skills::finalize_prepared_skill_commit(result);
            }
            if let Err(error) = pioneer_skills::finalize_reversible_skill_removals(&removals) {
                warn!(
                    pack_id = %next_parent.pack_id,
                    error = %format!("{error:#}"),
                    "failed to finalize staged removals after committed pack update"
                );
            }
            for removed in &diff.removed {
                if let Some(parent) = Path::new(removed.install_path.as_str()).parent() {
                    let _ = std::fs::remove_dir(parent);
                }
            }
            self.cleanup_upload_artifacts(
                &materialized.upload,
                materialized.cleanup_root.as_path(),
            );
            drop(write_guard);
            drop(upload_guard);

            let pack_item = SkillPackInstallationItem {
                id: next_parent.pack_id.clone(),
                name: next_parent.name.clone(),
                source_kind: next_parent.source_kind.clone(),
                created_at: next_parent.created_at_unix,
                updated_at: next_parent.updated_at_unix,
            };
            let mut ordered_changes = Vec::new();
            for child in &next_children {
                let key = child
                    .pack_member_key
                    .clone()
                    .expect("updated child member key");
                let previous = old_by_key.get(key.as_str()).copied();
                ordered_changes.push((
                    key,
                    child.skill_id.clone(),
                    SkillChangedItem {
                        skill_id: child.skill_id.clone(),
                        owner: child.owner.clone(),
                        slug: child.slug.clone(),
                        source_kind: child.source_kind.clone(),
                        change_type: if previous.is_some() {
                            "update".to_owned()
                        } else {
                            "install".to_owned()
                        },
                        fingerprint_before: previous.map(|child| child.fingerprint.clone()),
                        fingerprint_after: Some(child.fingerprint.clone()),
                    },
                ));
            }
            for child in &diff.removed {
                ordered_changes.push((
                    child
                        .pack_member_key
                        .clone()
                        .expect("removed child member key"),
                    child.skill_id.clone(),
                    SkillChangedItem {
                        skill_id: child.skill_id.clone(),
                        owner: child.owner.clone(),
                        slug: child.slug.clone(),
                        source_kind: child.source_kind.clone(),
                        change_type: "uninstall".to_owned(),
                        fingerprint_before: Some(child.fingerprint.clone()),
                        fingerprint_after: None,
                    },
                ));
            }
            ordered_changes
                .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
            let child_changes = ordered_changes
                .into_iter()
                .map(|(_, _, change)| change)
                .collect::<Vec<_>>();
            let payload = SkillsPackUpdateResponse {
                status: "updated".to_owned(),
                pack: pack_item,
                skills: next_children.iter().map(lifecycle_result).collect(),
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
                            "failed to send skills/pack/update response"
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
                            "failed to encode skills/pack/update response",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                }
            }
            self.notify_skill_projection_changed(
                workspace_id.as_str(),
                "pack_updated",
                child_changes,
                vec![SkillPackChangedItem {
                    pack_id: next_parent.pack_id,
                    change_type: "updated".to_owned(),
                    name_before: Some(expected_parent.name),
                    name_after: Some(next_parent.name),
                }],
                now,
            )
            .await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{PackMemberDiff, build_pack_member_diff};
    use pioneer_crud::SkillInstallationRecord;
    use pioneer_protocol::{SkillId, SkillPackId};

    fn child(id: char, key: &str) -> SkillInstallationRecord {
        SkillInstallationRecord {
            skill_id: SkillId::new(id.to_string().repeat(21)).expect("skill id"),
            owner: None,
            slug: key.to_owned(),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: "workspace-one".to_owned(),
            source_ref: "upload:old".to_owned(),
            install_path: format!("/managed/{id}/{key}"),
            trust_level: "community".to_owned(),
            fingerprint: format!("fingerprint-{key}"),
            updated_at_unix: 1,
            pack_id: Some(SkillPackId::new("P".repeat(21)).expect("pack id")),
            pack_member_key: Some(key.to_owned()),
        }
    }

    #[test]
    fn diff_matches_only_member_keys_and_orders_each_partition() {
        let alpha = child('A', "alpha");
        let removed = child('R', "removed");
        let zeta = child('Z', "zeta");
        let diff = build_pack_member_diff(
            &[zeta.clone(), removed.clone(), alpha.clone()],
            &["alpha".to_owned(), "new".to_owned(), "zeta".to_owned()],
        )
        .expect("diff");

        assert_eq!(
            diff,
            PackMemberDiff {
                retained: vec![alpha, zeta],
                added_keys: vec!["new".to_owned()],
                removed: vec![removed],
            }
        );
    }

    #[test]
    fn directory_rename_is_remove_plus_add_even_with_same_skill_metadata() {
        let old = child('A', "old-key");
        let diff = build_pack_member_diff(&[old.clone()], &["new-key".to_owned()]).expect("diff");
        assert!(diff.retained.is_empty());
        assert_eq!(diff.added_keys, vec!["new-key"]);
        assert_eq!(diff.removed, vec![old]);
    }
}
