use super::pack_install::with_pack_rollback_error;
use super::*;
use anyhow::{Result, anyhow};

fn rollback_pack_uninstall(
    request_id: &str,
    removals: &pioneer_skills::ReversibleSkillRemovalBatch,
) -> Result<()> {
    pack_uninstall_failpoint(request_id, "rollback", 0)?;
    pioneer_skills::rollback_reversible_skill_removals(removals)
}

#[cfg(not(test))]
fn pack_uninstall_failpoint(_request_id: &str, _stage: &str, _index: usize) -> Result<()> {
    Ok(())
}

#[cfg(test)]
static PACK_UNINSTALL_FAILPOINTS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Vec<(&'static str, usize)>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn pack_uninstall_failpoint(request_id: &str, stage: &str, index: usize) -> Result<()> {
    let failpoints = PACK_UNINSTALL_FAILPOINTS
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut failpoints = failpoints.lock().expect("pack uninstall failpoint lock");
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
    anyhow::bail!("injected pack uninstall failure at {stage}[{index}]")
}

impl MessageProcessor {
    #[cfg(test)]
    pub(in crate::message) fn set_pack_uninstall_failpoints(
        &self,
        request_id: &str,
        points: Vec<(&'static str, usize)>,
    ) {
        let failpoints = PACK_UNINSTALL_FAILPOINTS
            .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        failpoints
            .lock()
            .expect("pack uninstall failpoint lock")
            .insert(request_id.to_owned(), points);
    }

    pub(crate) fn skills_pack_uninstall<'a>(
        &'a self,
        request_context: &'a RequestContext,
        request_id: RequestId,
        params: SkillsPackUninstallParams,
    ) -> MessageFuture<'a, ()> {
        let connection_id = request_context.connection_id();
        Box::pin(async move {
            let workspace_id = match self
                .validate_skills_workspace(
                    connection_id,
                    request_id.clone(),
                    params.workspace_id,
                    methods::SKILLS_PACK_UNINSTALL,
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

            let write_guard = self.acquire_skills_write_lock().await;
            let parent = match self
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
            let children = match self
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

            let mut removals = None;
            if !children.is_empty() {
                let (source_kind, location) = match install_location_for_stored_source_kind(
                    &context,
                    parent.source_kind.as_str(),
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
                if children
                    .iter()
                    .any(|child| child.source_kind != parent.source_kind)
                {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "skill pack contains a child with a mismatched lifecycle source",
                            json!({"pack_id": parent.pack_id}),
                        ),
                    )
                    .await;
                    return;
                }
                if let Err(error) = self
                    .ensure_skills_lock_v2_locked(
                        location.lock_path.as_path(),
                        source_kind,
                        workspace_id.as_str(),
                    )
                    .await
                {
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
                let targets = children
                    .iter()
                    .map(|child| {
                        let managed_install_path =
                            pioneer_owned_install_path(&location, child.install_path.as_str())
                                .ok_or_else(|| {
                                    anyhow!(
                                        "pack child `{}` has an unmanaged install path",
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
                let targets = match targets {
                    Ok(targets) => targets,
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to prepare skill pack removal",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                };
                match pioneer_skills::stage_reversible_skill_removals(
                    pioneer_skills::StageReversibleSkillRemovalsRequest {
                        targets,
                        install_root: location.install_root,
                        lock_path: location.lock_path,
                    },
                ) {
                    Ok(staged) => removals = Some(staged),
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            skills_error(
                                Some(request_id),
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to stage skill pack removal",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await;
                        return;
                    }
                }
            }

            let now = now_timestamp_secs();
            let audit_records = children
            .iter()
            .map(|child| {
                let removal = removals.as_ref().and_then(|batch| {
                    batch
                        .removals
                        .iter()
                        .find(|removal| removal.skill_id == child.skill_id)
                });
                pioneer_skills::SkillAuditEvent::uninstall(
                    child.skill_id.clone(),
                    child.owner.clone(),
                    child.slug.clone(),
                    child.source_kind.clone(),
                    json!({
                        "install_path": child.install_path,
                        "was_present_on_disk": removal.is_some_and(|removal| removal.was_present_on_disk),
                        "had_lock_entry": removal.is_some_and(|removal| removal.removed_lock_entry.is_some()),
                        "managed_path_removed": true,
                        "pack_id": parent.pack_id,
                        "pack_member_key": child.pack_member_key,
                    }),
                    now,
                )
            })
            .collect::<Vec<_>>();
            let audit_records = skill_audit_records(audit_records.as_slice());

            let injected_error = removals
                .as_ref()
                .and_then(|_| pack_uninstall_failpoint(request_id.as_str(), "persist", 0).err());
            let persisted = if let Some(error) = injected_error {
                Err(error)
            } else {
                self.crud_store
                    .uninstall_skill_pack_lifecycle(
                        &parent,
                        children.as_slice(),
                        audit_records.as_slice(),
                    )
                    .await
            };
            let persistence_error = match persisted {
                Ok(true) => None,
                Ok(false) => Some(anyhow!("skill pack changed before uninstall publication")),
                Err(error) => Some(error),
            };
            if let Some(primary) = persistence_error {
                let error = if let Some(removals) = removals.as_ref() {
                    with_pack_rollback_error(
                        primary,
                        rollback_pack_uninstall(request_id.as_str(), removals),
                    )
                } else {
                    primary
                };
                drop(write_guard);
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to persist skill pack uninstall",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }

            if let Some(removals) = removals.as_ref()
                && let Err(error) = pioneer_skills::finalize_reversible_skill_removals(removals)
            {
                warn!(
                    pack_id = %parent.pack_id,
                    error = %format!("{error:#}"),
                    "failed to finalize staged removals after committed pack uninstall"
                );
            }
            for child in &children {
                if let Some(id_root) = Path::new(child.install_path.as_str()).parent() {
                    let _ = std::fs::remove_dir(id_root);
                }
            }
            drop(write_guard);

            let pack = SkillPackInstallationItem {
                id: parent.pack_id.clone(),
                name: parent.name.clone(),
                source_kind: parent.source_kind.clone(),
                created_at: parent.created_at_unix,
                updated_at: parent.updated_at_unix,
            };
            let removed_skills = children
                .iter()
                .map(|child| SkillLifecycleRemovedSkill {
                    skill_id: child.skill_id.clone(),
                    owner: child.owner.clone(),
                    slug: child.slug.clone(),
                    source_kind: child.source_kind.clone(),
                    removed_install_path: Some(child.install_path.clone()),
                })
                .collect::<Vec<_>>();
            let child_changes = children
                .iter()
                .map(|child| SkillChangedItem {
                    skill_id: child.skill_id.clone(),
                    owner: child.owner.clone(),
                    slug: child.slug.clone(),
                    source_kind: child.source_kind.clone(),
                    change_type: "uninstall".to_owned(),
                    fingerprint_before: Some(child.fingerprint.clone()),
                    fingerprint_after: None,
                })
                .collect::<Vec<_>>();
            let payload = SkillsPackUninstallResponse {
                status: "uninstalled".to_owned(),
                pack,
                removed_skills,
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
                            "failed to send skills/pack/uninstall response"
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
                            "failed to encode skills/pack/uninstall response",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                }
            }
            self.notify_skill_projection_changed(
                workspace_id.as_str(),
                "pack_uninstalled",
                child_changes,
                vec![SkillPackChangedItem {
                    pack_id: parent.pack_id,
                    change_type: "uninstalled".to_owned(),
                    name_before: Some(parent.name),
                    name_after: None,
                }],
                now,
            )
            .await;
        })
    }
}
