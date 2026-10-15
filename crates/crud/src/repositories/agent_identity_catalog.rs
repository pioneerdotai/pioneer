//! Persistence for the canonical Agent identities derived from application settings.

use super::agent_domain::{
    AgentIdentityInput, NICKNAME_OWNER_RESERVED, NativeAgentConfigInput, PresentationSnapshotInput,
    SOURCE_CLI_RUNTIME_INSTANCE, SOURCE_NATIVE_AGENT, claim_actor_nickname, ensure_agent_identity,
    ensure_native_agent_config, insert_presentation_snapshot, load_agent_identity_by_source,
};
use anyhow::{Context, Result, bail};
use chrono::DateTime;
use pioneer_entity::{
    actor_nickname_index, agent_identity, agent_presentation_snapshot, native_agent_config,
    workspace,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const PIONEER_SYSTEM_KEY: &str = "pioneer";
const PIONEER_NICKNAME: &str = "pioneer";
const PIONEER_FINGERPRINT: &str = "seed:pioneer:v1";
const CATALOG_PAGE_SIZE: u64 = 256;
const MAX_RUNTIME_INSTANCES: usize = pioneer_protocol::ChildAgentLaunchGrantSet::MAX_IDENTITIES;

/// Provider-independent identity fields derived from one configured CLI runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeIdentitySeed {
    pub id: String,
    pub kind: String,
    pub display_name: String,
    pub nickname: String,
    pub enabled: bool,
    /// Canonical non-secret settings. Only their digest is persisted.
    pub source_revision_material: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentIdentityCatalogSyncReport {
    pub workspace_count: u64,
    pub cli_instance_count: u64,
    pub cli_identity_count: u64,
    pub presentation_snapshot_count: u64,
}

/// Establishes the built-in Pioneer identity invariant for a workspace.
pub async fn ensure_pioneer_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTime<chrono::FixedOffset>,
) -> Result<()> {
    let configs = native_agent_config::Entity::find()
        .filter(native_agent_config::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(native_agent_config::Column::SystemKey.eq(PIONEER_SYSTEM_KEY.to_owned()))
        .limit(2)
        .all(db)
        .await
        .context("failed to inspect reserved Pioneer native-agent configs")?;
    if configs.len() > 1 {
        bail!(
            "workspace `{workspace_id}` has {} reserved Pioneer native-agent configs",
            configs.len()
        );
    }

    let config = if let Some(config) = configs.into_iter().next() {
        if config.display_name != "Pioneer"
            || config.nickname != PIONEER_NICKNAME
            || !config.enabled
        {
            bail!("workspace `{workspace_id}` has a conflicting reserved Pioneer config");
        }
        config
    } else {
        ensure_native_agent_config(
            db,
            &NativeAgentConfigInput {
                id: deterministic_id('N', &format!("pioneer-config\0{workspace_id}")),
                workspace_id: workspace_id.to_owned(),
                system_key: Some(PIONEER_SYSTEM_KEY.to_owned()),
                display_name: "Pioneer".to_owned(),
                nickname: PIONEER_NICKNAME.to_owned(),
                enabled: true,
                avatar_revision: None,
                config_revision: 1,
                now: now.clone().into(),
            },
        )
        .await?
    };

    let identities = agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(agent_identity::Column::SourceKind.eq(SOURCE_NATIVE_AGENT.to_owned()))
        .filter(agent_identity::Column::SourceId.eq(config.id.clone()))
        .limit(2)
        .all(db)
        .await
        .context("failed to inspect reserved Pioneer identities")?;
    if identities.len() > 1 {
        bail!("workspace `{workspace_id}` has duplicate identities for reserved Pioneer config");
    }
    let identity = if let Some(identity) = identities.into_iter().next() {
        if identity.source_fingerprint != PIONEER_FINGERPRINT
            || identity.status != "active"
            || identity.retired_at.is_some()
        {
            bail!("workspace `{workspace_id}` has a conflicting reserved Pioneer identity");
        }
        identity
    } else {
        ensure_agent_identity(
            db,
            &AgentIdentityInput {
                id: deterministic_id('A', &format!("pioneer-identity\0{workspace_id}")),
                workspace_id: workspace_id.to_owned(),
                source_kind: SOURCE_NATIVE_AGENT.to_owned(),
                source_id: config.id.clone(),
                source_revision: 1,
                source_fingerprint: PIONEER_FINGERPRINT.to_owned(),
                now: now.clone().into(),
            },
        )
        .await?
    };

    let nickname = actor_nickname_index::Entity::find_by_id((
        workspace_id.to_owned(),
        PIONEER_NICKNAME.to_owned(),
    ))
    .one(db)
    .await
    .context("failed to inspect reserved Pioneer nickname")?;
    match nickname {
        Some(row)
            if row.owner_kind == NICKNAME_OWNER_RESERVED
                && row.owner_id == PIONEER_NICKNAME
                && row.status == "active" => {}
        Some(_) => bail!("workspace `{workspace_id}` has a conflicting Pioneer nickname"),
        None => {
            claim_actor_nickname(
                db,
                workspace_id,
                PIONEER_NICKNAME,
                NICKNAME_OWNER_RESERVED,
                PIONEER_NICKNAME,
                now.clone().into(),
            )
            .await?;
        }
    }

    let snapshots = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(identity.id.clone()))
        .filter(
            agent_presentation_snapshot::Column::SourceFingerprint
                .eq(PIONEER_FINGERPRINT.to_owned()),
        )
        .limit(2)
        .all(db)
        .await
        .context("failed to inspect reserved Pioneer presentation snapshots")?;
    if snapshots.len() > 1 {
        bail!("workspace `{workspace_id}` has duplicate Pioneer presentation snapshots");
    }
    if snapshots.is_empty() {
        insert_presentation_snapshot(
            db,
            &PresentationSnapshotInput {
                id: deterministic_id('S', &format!("pioneer-snapshot\0{workspace_id}")),
                agent_identity_id: identity.id,
                source_revision: 1,
                source_fingerprint: PIONEER_FINGERPRINT.to_owned(),
                display_name: "Pioneer".to_owned(),
                nickname: PIONEER_NICKNAME.to_owned(),
                avatar_revision: None,
                role_label: None,
                now: now.into(),
            },
        )
        .await?;
    }
    Ok(())
}

/// Synchronizes the canonical CLI settings with Agent identities in every workspace.
pub async fn sync_cli_runtime_identity_catalog<C: ConnectionTrait>(
    db: &C,
    instances: &[CliRuntimeIdentitySeed],
    now: DateTime<chrono::FixedOffset>,
) -> Result<AgentIdentityCatalogSyncReport> {
    validate_runtime_catalog(instances)?;
    let configured_source_ids = instances
        .iter()
        .map(|instance| instance.id.as_str())
        .collect::<BTreeSet<_>>();
    let workspaces = workspace::Entity::find()
        .order_by_asc(workspace::Column::Id)
        .paginate(db, CATALOG_PAGE_SIZE);
    let workspace_pages = workspaces
        .num_pages()
        .await
        .context("failed to count workspaces for CLI identity synchronization")?;
    let mut report = AgentIdentityCatalogSyncReport::default();
    for page in 0..workspace_pages {
        for workspace in workspaces
            .fetch_page(page)
            .await
            .context("failed to page workspaces for CLI identity synchronization")?
        {
            ensure_pioneer_for_workspace(db, workspace.id.as_str(), now.clone()).await?;
            report.workspace_count = report.workspace_count.saturating_add(1);
            for instance in instances {
                project_cli_runtime_identity(
                    db,
                    workspace.id.as_str(),
                    instance,
                    now.clone(),
                    &mut report,
                )
                .await?;
            }
            loop {
                let mut removed_query = agent_identity::Entity::find()
                    .filter(agent_identity::Column::WorkspaceId.eq(workspace.id.clone()))
                    .filter(agent_identity::Column::SourceKind.eq(SOURCE_CLI_RUNTIME_INSTANCE))
                    .filter(agent_identity::Column::Status.eq("active"));
                if !configured_source_ids.is_empty() {
                    removed_query = removed_query.filter(
                        agent_identity::Column::SourceId.is_not_in(
                            configured_source_ids
                                .iter()
                                .map(|source_id| (*source_id).to_owned()),
                        ),
                    );
                }
                let removed_identities = removed_query
                    .order_by_asc(agent_identity::Column::Id)
                    .limit(CATALOG_PAGE_SIZE)
                    .all(db)
                    .await
                    .context("failed to inspect removed CLI runtime identities")?;
                if removed_identities.is_empty() {
                    break;
                }
                for identity in removed_identities {
                    let retired = agent_identity::Entity::update_many()
                        .col_expr(
                            agent_identity::Column::Status,
                            sea_orm::sea_query::Expr::value("retired"),
                        )
                        .col_expr(
                            agent_identity::Column::RetiredAt,
                            sea_orm::sea_query::Expr::value(Some(now.clone())),
                        )
                        .col_expr(
                            agent_identity::Column::UpdatedAt,
                            sea_orm::sea_query::Expr::value(now.clone()),
                        )
                        .filter(agent_identity::Column::Id.eq(identity.id.clone()))
                        .filter(agent_identity::Column::Status.eq("active"))
                        .exec(db)
                        .await
                        .context("failed to retire removed CLI runtime identity")?;
                    if retired.rows_affected != 1 {
                        bail!("CLI runtime identity changed concurrently while being retired");
                    }
                    actor_nickname_index::Entity::update_many()
                        .col_expr(
                            actor_nickname_index::Column::Status,
                            sea_orm::sea_query::Expr::value(
                                super::agent_domain::NICKNAME_TOMBSTONED,
                            ),
                        )
                        .col_expr(
                            actor_nickname_index::Column::TombstonedAt,
                            sea_orm::sea_query::Expr::value(Some(now.clone())),
                        )
                        .filter(actor_nickname_index::Column::WorkspaceId.eq(workspace.id.clone()))
                        .filter(actor_nickname_index::Column::OwnerKind.eq("agent"))
                        .filter(actor_nickname_index::Column::OwnerId.eq(identity.id))
                        .filter(actor_nickname_index::Column::Status.eq("active"))
                        .exec(db)
                        .await
                        .context("failed to tombstone removed CLI runtime nickname")?;
                }
            }
        }
    }
    Ok(report)
}

async fn project_cli_runtime_identity<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    instance: &CliRuntimeIdentitySeed,
    now: DateTime<chrono::FixedOffset>,
    report: &mut AgentIdentityCatalogSyncReport,
) -> Result<()> {
    let nickname = configured_runtime_nickname(instance.nickname.as_str(), instance.id.as_str())?;
    if nickname == PIONEER_NICKNAME {
        bail!(
            "CLI runtime `{}` conflicts with reserved Pioneer nickname",
            instance.id
        );
    }
    let fingerprint = cli_runtime_identity_fingerprint(instance);
    let existing = load_agent_identity_by_source(
        db,
        workspace_id,
        SOURCE_CLI_RUNTIME_INSTANCE,
        instance.id.as_str(),
    )
    .await?;
    let identity = if let Some(existing) = existing {
        if existing.status != "active" || existing.retired_at.is_some() {
            bail!(
                "CLI runtime `{}` maps to a retired identity in workspace `{workspace_id}`",
                instance.id
            );
        }
        if existing.source_fingerprint == fingerprint {
            existing
        } else {
            let next_revision = existing
                .source_revision
                .checked_add(1)
                .context("CLI runtime identity source revision exhausted")?;
            let updated = agent_identity::Entity::update_many()
                .col_expr(
                    agent_identity::Column::SourceRevision,
                    sea_orm::sea_query::Expr::value(next_revision),
                )
                .col_expr(
                    agent_identity::Column::SourceFingerprint,
                    sea_orm::sea_query::Expr::value(fingerprint.clone()),
                )
                .col_expr(
                    agent_identity::Column::UpdatedAt,
                    sea_orm::sea_query::Expr::value(now.clone()),
                )
                .filter(agent_identity::Column::Id.eq(existing.id.clone()))
                .filter(agent_identity::Column::SourceRevision.eq(existing.source_revision))
                .filter(agent_identity::Column::Status.eq("active"))
                .exec(db)
                .await
                .context("failed to update CLI runtime identity projection")?;
            if updated.rows_affected != 1 {
                bail!("CLI runtime identity source changed concurrently");
            }
            agent_identity::Entity::find_by_id(existing.id)
                .one(db)
                .await
                .context("failed to reload CLI runtime identity projection")?
                .context("CLI runtime identity disappeared after update")?
        }
    } else {
        ensure_agent_identity(
            db,
            &AgentIdentityInput {
                id: deterministic_id(
                    'C',
                    &format!("cli-runtime\0{workspace_id}\0{}", instance.id),
                ),
                workspace_id: workspace_id.to_owned(),
                source_kind: SOURCE_CLI_RUNTIME_INSTANCE.to_owned(),
                source_id: instance.id.clone(),
                source_revision: 1,
                source_fingerprint: fingerprint.clone(),
                now: now.clone().into(),
            },
        )
        .await?
    };

    claim_actor_nickname(
        db,
        workspace_id,
        nickname.as_str(),
        "agent",
        identity.id.as_str(),
        now.clone().into(),
    )
    .await
    .with_context(|| {
        format!(
            "CLI runtime `{}` cannot activate because nickname `{nickname}` is already owned",
            instance.id
        )
    })?;

    // A return to earlier settings repeats the fingerprint, but is a new source
    // revision. Keep existing snapshot IDs (including the legacy fingerprint-only
    // IDs) so historical execution references remain immutable. New snapshots
    // must include the revision to avoid colliding with that earlier history.
    let snapshot_id = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(identity.id.clone()))
        .filter(agent_presentation_snapshot::Column::SourceRevision.eq(identity.source_revision))
        .filter(agent_presentation_snapshot::Column::SourceFingerprint.eq(fingerprint.clone()))
        .one(db)
        .await
        .context("failed to inspect CLI runtime presentation snapshot")?
        .map(|snapshot| snapshot.id)
        .unwrap_or_else(|| {
            deterministic_id(
                'S',
                &format!(
                    "cli-snapshot\0{workspace_id}\0{}\0{}\0{fingerprint}",
                    instance.id, identity.source_revision
                ),
            )
        });
    insert_presentation_snapshot(
        db,
        &PresentationSnapshotInput {
            id: snapshot_id,
            agent_identity_id: identity.id,
            source_revision: identity.source_revision,
            source_fingerprint: fingerprint,
            display_name: instance.display_name.clone(),
            nickname,
            avatar_revision: None,
            role_label: Some(instance.kind.clone()),
            now: now.into(),
        },
    )
    .await?;

    report.cli_instance_count = report.cli_instance_count.saturating_add(1);
    report.cli_identity_count = report.cli_identity_count.saturating_add(1);
    report.presentation_snapshot_count = report.presentation_snapshot_count.saturating_add(1);
    Ok(())
}

fn validate_runtime_catalog(instances: &[CliRuntimeIdentitySeed]) -> Result<()> {
    if instances.len() > MAX_RUNTIME_INSTANCES {
        bail!("CLI runtime catalog exceeds the bounded identity limit");
    }
    let mut ids = BTreeSet::new();
    let mut nicknames = BTreeMap::<String, String>::new();
    for instance in instances {
        if instance.source_revision_material.is_empty() {
            bail!(
                "CLI runtime `{}` has no canonical source revision material",
                instance.id
            );
        }
        let id = instance.id.trim();
        if id.is_empty() || id.len() > 255 {
            bail!("CLI runtime identity id must be non-empty and at most 255 bytes");
        }
        if !ids.insert(id.to_owned()) {
            bail!("duplicate CLI runtime identity id `{id}`");
        }
        let nickname = configured_runtime_nickname(instance.nickname.as_str(), id)?;
        if let Some(previous) = nicknames.insert(nickname.clone(), id.to_owned()) {
            bail!("CLI runtime ids `{previous}` and `{id}` map to the same nickname `{nickname}`");
        }
    }
    Ok(())
}

fn runtime_nickname(id: &str) -> String {
    let mut nickname = id
        .trim()
        .chars()
        .map(|character| {
            let character = character.to_ascii_lowercase();
            if character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
            {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    nickname.truncate(32);
    if nickname.len() < 2 {
        format!("cli-{}", &short_hash(id)[..8])
    } else {
        nickname
    }
}

fn configured_runtime_nickname(configured: &str, id: &str) -> Result<String> {
    let configured = configured.trim();
    let configured = configured.strip_prefix('@').unwrap_or(configured);
    if configured.is_empty() {
        return Ok(runtime_nickname(id));
    }
    pioneer_protocol::AgentNicknameKey::new(configured.to_owned())
        .map(|nickname| nickname.as_str().to_owned())
        .map_err(|error| anyhow::anyhow!("invalid CLI runtime nickname `{configured}`: {error}"))
}

pub fn cli_runtime_identity_fingerprint(instance: &CliRuntimeIdentitySeed) -> String {
    let mut digest = Sha256::new();
    digest.update(b"pioneer:agent-runtime:cli-runtime-identity:v2\0");
    digest.update(instance.id.as_bytes());
    digest.update([0]);
    digest.update(instance.kind.as_bytes());
    digest.update([0]);
    digest.update(instance.display_name.as_bytes());
    digest.update([0]);
    digest.update(instance.nickname.as_bytes());
    digest.update([0]);
    digest.update([instance.enabled as u8]);
    digest.update([0]);
    digest.update(instance.source_revision_material.as_bytes());
    format!("cli-runtime:v2:{}", hex::encode(digest.finalize()))
}

fn deterministic_id(prefix: char, key: &str) -> String {
    format!("{prefix}{}", &short_hash(key)[..20])
}

fn short_hash(value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(value.as_bytes());
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::{CliRuntimeIdentitySeed, runtime_nickname, validate_runtime_catalog};

    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::{agent_identity, agent_presentation_snapshot, workspace};
    use pioneer_sqlite::SqliteDatabase;
    use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Set};

    async fn catalog_database() -> SqliteDatabase {
        let connection = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&connection, None).await.unwrap();
        let db = SqliteDatabase::from_single_connection(connection);
        workspace::ActiveModel {
            id: Set("workspace".into()),
            name: Set("Workspace".into()),
            is_active: Set(true),
            is_current: Set(true),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        db
    }

    fn runtime_seed(kind: &str, enabled: bool) -> CliRuntimeIdentitySeed {
        CliRuntimeIdentitySeed {
            id: kind.into(),
            kind: kind.into(),
            display_name: format!("{kind} CLI"),
            nickname: kind.into(),
            enabled,
            source_revision_material: format!("{kind}:{enabled}"),
        }
    }

    async fn sync(db: &SqliteDatabase, instances: &[CliRuntimeIdentitySeed]) {
        super::sync_cli_runtime_identity_catalog(db, instances, chrono::Utc::now().fixed_offset())
            .await
            .expect("CLI settings projection must succeed");
    }

    #[tokio::test]
    async fn cli_provider_off_on_cycles_keep_immutable_history_and_are_idempotent() {
        for kind in ["codex", "claude"] {
            let db = catalog_database().await;
            let mut previous = Vec::new();
            for (index, enabled) in [true, false, true, false, true].into_iter().enumerate() {
                // Keep both sources configured so switching one does not retire the other.
                let instances = [
                    runtime_seed("codex", enabled),
                    runtime_seed("claude", enabled),
                ];
                sync(&db, &instances).await;
                let identity = super::load_agent_identity_by_source(
                    &db,
                    "workspace",
                    super::SOURCE_CLI_RUNTIME_INSTANCE,
                    kind,
                )
                .await
                .unwrap()
                .unwrap();
                let snapshots = agent_presentation_snapshot::Entity::find()
                    .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(&identity.id))
                    .order_by_asc(agent_presentation_snapshot::Column::SourceRevision)
                    .all(&db)
                    .await
                    .unwrap();
                if index > 0 {
                    assert_eq!(&snapshots[..previous.len()], previous.as_slice());
                }
                assert_eq!(
                    snapshots.last().unwrap().source_revision,
                    identity.source_revision
                );
                assert_eq!(
                    snapshots.last().unwrap().source_fingerprint,
                    identity.source_fingerprint
                );
                assert_eq!(identity.status, "active");
                assert_eq!(snapshots.len(), index + 1);
                sync(&db, &instances).await;
                let replay = agent_presentation_snapshot::Entity::find()
                    .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(&identity.id))
                    .order_by_asc(agent_presentation_snapshot::Column::SourceRevision)
                    .all(&db)
                    .await
                    .unwrap();
                assert_eq!(
                    snapshots, replay,
                    "replay must not rewrite or append history"
                );
                previous = snapshots;
            }
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cli_provider_projection_recovers_after_failed_reenable_and_rollback() {
        for partial_revision in [3, 4] {
            let db = catalog_database().await;
            let on = runtime_seed("codex", true);
            let off = runtime_seed("codex", false);
            sync(&db, &[on.clone()]).await;
            sync(&db, &[off.clone()]).await;
            let identity = super::load_agent_identity_by_source(
                &db,
                "workspace",
                super::SOURCE_CLI_RUNTIME_INSTANCE,
                "codex",
            )
            .await
            .unwrap()
            .unwrap();
            // Old code advanced the identity before its snapshot insert failed. The
            // rollback also failed, leaving this identity ahead of the settings file.
            let mut partial: agent_identity::ActiveModel = identity.into();
            partial.source_revision = Set(partial_revision);
            partial.source_fingerprint = Set(super::cli_runtime_identity_fingerprint(
                if partial_revision == 3 { &on } else { &off },
            ));
            partial.update(&db).await.unwrap();
            sync(&db, &[off]).await;
            sync(&db, &[on]).await;
            let identity = super::load_agent_identity_by_source(
                &db,
                "workspace",
                super::SOURCE_CLI_RUNTIME_INSTANCE,
                "codex",
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(identity.source_revision, 5);
            let history = agent_presentation_snapshot::Entity::find()
                .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(identity.id))
                .order_by_asc(agent_presentation_snapshot::Column::SourceRevision)
                .all(&db)
                .await
                .unwrap();
            assert_eq!(
                history
                    .iter()
                    .map(|row| row.source_revision)
                    .collect::<Vec<_>>(),
                vec![1, 2, 4, 5]
            );
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cli_provider_projection_preserves_legacy_snapshot_ids() {
        let db = catalog_database().await;
        let on = runtime_seed("codex", true);
        sync(&db, &[on.clone()]).await;
        let identity = super::load_agent_identity_by_source(
            &db,
            "workspace",
            super::SOURCE_CLI_RUNTIME_INSTANCE,
            "codex",
        )
        .await
        .unwrap()
        .unwrap();
        let legacy_id = super::deterministic_id(
            'S',
            &format!(
                "cli-snapshot\0workspace\0codex\0{}",
                identity.source_fingerprint,
            ),
        );
        agent_presentation_snapshot::Entity::update_many()
            .col_expr(
                agent_presentation_snapshot::Column::Id,
                sea_orm::sea_query::Expr::value(&legacy_id),
            )
            .filter(agent_presentation_snapshot::Column::AgentIdentityId.eq(&identity.id))
            .exec(&db)
            .await
            .unwrap();
        let before = agent_presentation_snapshot::Entity::find_by_id(&legacy_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        sync(&db, &[on]).await;
        let after = agent_presentation_snapshot::Entity::find_by_id(&legacy_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before, after);
        db.close().await.unwrap();
    }

    #[test]
    fn runtime_nickname_is_stable_and_bounded() {
        assert_eq!(runtime_nickname("Codex/Main"), "codex-main");
        assert!(runtime_nickname("x").len() >= 2);
        assert!(runtime_nickname(&"x".repeat(100)).len() <= 32);
    }

    #[test]
    fn runtime_catalog_rejects_duplicate_nicknames() {
        let instances = [
            CliRuntimeIdentitySeed {
                id: "first".to_owned(),
                kind: "codex".to_owned(),
                display_name: "First".to_owned(),
                nickname: "shared".to_owned(),
                enabled: true,
                source_revision_material: "first".to_owned(),
            },
            CliRuntimeIdentitySeed {
                id: "second".to_owned(),
                kind: "claude".to_owned(),
                display_name: "Second".to_owned(),
                nickname: "shared".to_owned(),
                enabled: true,
                source_revision_material: "second".to_owned(),
            },
        ];
        assert!(validate_runtime_catalog(&instances).is_err());
    }
}
