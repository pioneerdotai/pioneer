use anyhow::Result;
use pioneer_crud_packages::skill_audit_event::SkillAuditEventRecord;
use pioneer_crud_packages::skill_pack_installation::SkillPackInstallationRecord;
use pioneer_crud_packages::skill_pack_transactions::{
    SkillPackRepository, skill_installation_record_from_model,
};
use pioneer_crud_packages::skill_records::{
    SkillInstallationPatch, SkillInstallationRecord, SkillUploadSessionRecord,
    WorkspaceSkillPolicyRecord,
};
use pioneer_crud_packages::skill_upload_session::skill_upload_session_record_from_model;
use pioneer_crud_packages::{
    skill_audit_event, skill_installation, skill_lifecycle, skill_reconciliation,
    skill_upload_session, skill_workspace_policy,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::TestDatabase;
use pioneer_protocol::{SkillId, SkillPackId, generate_id};
use sea_orm::ConnectionTrait;

fn unix_to_datetime(seconds: i64) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::from_timestamp(seconds, 0)
        .expect("fixture timestamp")
        .fixed_offset()
}

async fn database(fixture: &TestDatabase) -> Result<PostgresDatabase> {
    Ok(PostgresDatabase::new(
        fixture.connect_read_only().await?,
        fixture.connection().clone(),
    ))
}

async fn find_installation(
    db: &PostgresDatabase,
    skill_id: &SkillId,
) -> Result<Option<SkillInstallationRecord>> {
    skill_installation::find_skill_installation(db, skill_id)
        .await?
        .map(skill_installation_record_from_model)
        .transpose()
}

async fn find_policy(
    db: &PostgresDatabase,
    workspace: &str,
    skill_id: &SkillId,
) -> Result<Option<WorkspaceSkillPolicyRecord>> {
    skill_workspace_policy::find_workspace_skill_policy(db, workspace, skill_id)
        .await?
        .map(skill_workspace_policy::workspace_skill_policy_record_from_model)
        .transpose()
}

async fn insert_upload(db: &PostgresDatabase, record: &SkillUploadSessionRecord) -> Result<()> {
    let now = unix_to_datetime(record.created_at_unix);
    skill_upload_session::insert_skill_upload_session(db, record, now, now).await
}

async fn find_upload(db: &PostgresDatabase, id: &str) -> Result<Option<SkillUploadSessionRecord>> {
    Ok(skill_upload_session::find_skill_upload_session(db, id)
        .await?
        .map(skill_upload_session_record_from_model))
}

async fn transition_upload(
    db: &PostgresDatabase,
    id: &str,
    expected: &[&str],
    status: &str,
    finalized_at: Option<i64>,
    consumed_at: Option<i64>,
    aborted_at: Option<i64>,
    updated_at: i64,
) -> Result<Option<SkillUploadSessionRecord>> {
    if !skill_upload_session::transition_skill_upload_status(
        db,
        id,
        expected,
        status,
        finalized_at,
        consumed_at,
        aborted_at,
        unix_to_datetime(updated_at),
    )
    .await?
    {
        return Ok(None);
    }
    find_upload(db, id).await
}

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

#[tokio::test]
async fn generic_skill_patch_cannot_change_pack_identity() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;
        let parent = skill_pack_record('P', "initial", "workspace-one");
        let child = pack_skill_record('A', &parent, "alpha");
        SkillPackRepository::new(&db)
            .insert_skill_pack_installation_with_children(&parent, std::slice::from_ref(&child))
            .await
            .unwrap();
        assert!(
            skill_reconciliation::update_skill_installation(
                &db,
                &child.skill_id,
                &SkillInstallationPatch {
                    pack_id: Some(None),
                    pack_member_key: Some(None),
                    ..Default::default()
                },
                1_700_000_200,
            )
            .await
            .is_err()
        );
        let unchanged = find_installation(&db, &child.skill_id)
            .await
            .expect("retained child should query")
            .expect("retained child should exist");
        assert_eq!(unchanged.pack_id, Some(parent.pack_id.clone()));
        assert_eq!(unchanged.pack_member_key.as_deref(), Some("alpha"));
        assert!(
            skill_reconciliation::update_skill_installation(
                &db,
                &child.skill_id,
                &SkillInstallationPatch {
                    source_kind: Some("registry".to_owned()),
                    ..Default::default()
                },
                1_700_000_201,
            )
            .await
            .is_err()
        );
        assert_eq!(
            find_installation(&db, &child.skill_id)
                .await
                .expect("retained child should query after rejected source change")
                .expect("retained child should exist after rejected source change")
                .source_kind,
            parent.source_kind
        );

        let mut standalone = pack_skill_record('S', &parent, "standalone");
        standalone.pack_id = None;
        standalone.pack_member_key = None;
        skill_installation::insert_standalone(&db, &standalone, unix_to_datetime(1_700_000_200))
            .await
            .expect("insert standalone skill");
        assert!(
            skill_reconciliation::update_skill_installation(
                &db,
                &standalone.skill_id,
                &SkillInstallationPatch {
                    pack_id: Some(Some(parent.pack_id.clone())),
                    pack_member_key: Some(Some("adopted".to_owned())),
                    ..Default::default()
                },
                1_700_000_300,
            )
            .await
            .is_err()
        );
        let standalone = find_installation(&db, &standalone.skill_id)
            .await
            .expect("standalone skill should query")
            .expect("standalone skill should remain");
        assert_eq!(standalone.pack_id, None);
        assert_eq!(standalone.pack_member_key, None);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn skill_upload_terminal_transitions_cannot_overwrite_consumed_or_aborted_state() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;
        fixture
            .connection()
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('workspace-one', 'Workspace')",
            )
            .await?;
        let finalized = |upload_id: &str| SkillUploadSessionRecord {
            purpose: "skill".to_owned(),
            upload_id: upload_id.to_owned(),
            workspace_id: "workspace-one".to_owned(),
            connection_id: 7,
            status: "finalized".to_owned(),
            file_name: "skills.tar.gz".to_owned(),
            archive_format: "tar_gz".to_owned(),
            compressed_size_bytes: 1,
            received_bytes: 1,
            sha256: "a".repeat(64),
            payload_path: "/tmp/skills.tar.gz".to_owned(),
            created_at_unix: 10,
            expires_at_unix: 100,
            finalized_at_unix: Some(11),
            consumed_at_unix: None,
            aborted_at_unix: None,
        };
        let consumed_id = "UUUUUUUUUUUUUUUUUUUUU";
        let aborted_id = "VVVVVVVVVVVVVVVVVVVVV";
        insert_upload(&db, &finalized(consumed_id))
            .await
            .expect("consumed fixture should insert");
        insert_upload(&db, &finalized(aborted_id))
            .await
            .expect("aborted fixture should insert");

        assert!(
            transition_upload(
                &db,
                consumed_id,
                &["finalized"],
                "consumed",
                None,
                Some(20),
                None,
                20,
            )
            .await
            .expect("consume transition should succeed")
            .is_some()
        );
        assert!(
            transition_upload(
                &db,
                consumed_id,
                &["finalized", "consumed"],
                "aborted",
                None,
                None,
                Some(21),
                21,
            )
            .await
            .expect("terminal overwrite should be rejected")
            .is_none()
        );

        assert!(
            transition_upload(
                &db,
                aborted_id,
                &["finalized"],
                "aborted",
                None,
                None,
                Some(30),
                30,
            )
            .await
            .expect("abort transition should succeed")
            .is_some()
        );
        assert!(
            transition_upload(
                &db,
                aborted_id,
                &["finalized", "aborted"],
                "consumed",
                None,
                Some(31),
                None,
                31,
            )
            .await
            .expect("aborted overwrite should be rejected")
            .is_none()
        );

        let consumed = find_upload(&db, consumed_id)
            .await
            .expect("consumed fixture should query")
            .expect("consumed fixture should remain");
        assert_eq!(consumed.status, "consumed");
        assert_eq!(consumed.consumed_at_unix, Some(20));
        assert_eq!(consumed.aborted_at_unix, None);
        assert!(
            insert_upload(&db, &finalized(consumed_id)).await.is_err(),
            "creating a colliding upload must not overwrite terminal state"
        );
        let consumed = find_upload(&db, consumed_id)
            .await
            .expect("consumed fixture should query after collision")
            .expect("consumed fixture should survive collision");
        assert_eq!(consumed.status, "consumed");
        assert_eq!(consumed.consumed_at_unix, Some(20));
        assert_eq!(consumed.aborted_at_unix, None);
        let aborted = find_upload(&db, aborted_id)
            .await
            .expect("aborted fixture should query")
            .expect("aborted fixture should remain");
        assert_eq!(aborted.status, "aborted");
        assert_eq!(aborted.consumed_at_unix, None);
        assert_eq!(aborted.aborted_at_unix, Some(30));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_skill_lifecycle_transactions_include_upload_consumption() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;
        fixture
            .connection()
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('workspace-one', 'Workspace')",
            )
            .await?;
        let upload = |upload_id: &str| SkillUploadSessionRecord {
            purpose: "skill".to_owned(),
            upload_id: upload_id.to_owned(),
            workspace_id: "workspace-one".to_owned(),
            connection_id: 7,
            status: "finalized".to_owned(),
            file_name: "skill.tar.gz".to_owned(),
            archive_format: "tar_gz".to_owned(),
            compressed_size_bytes: 1,
            received_bytes: 1,
            sha256: "a".repeat(64),
            payload_path: "/tmp/skill.tar.gz".to_owned(),
            created_at_unix: 10,
            expires_at_unix: 100,
            finalized_at_unix: Some(11),
            consumed_at_unix: None,
            aborted_at_unix: None,
        };
        let parent = skill_pack_record('P', "unused", "workspace-one");
        let mut installation = pack_skill_record('S', &parent, "standalone");
        installation.pack_id = None;
        installation.pack_member_key = None;
        let policy = WorkspaceSkillPolicyRecord {
            id: "IIIIIIIIIIIIIIIIIIIII".to_owned(),
            workspace_id: installation.scope_key.clone(),
            skill_id: installation.skill_id.clone(),
            enabled: Some(true),
            allow_implicit_invocation: Some(false),
        };
        let audit = |action: &str, fingerprint: &str, timestamp: i64| SkillAuditEventRecord {
            turn_id: None,
            skill_id: installation.skill_id.clone(),
            skill_owner: installation.owner.clone(),
            skill_slug: installation.slug.clone(),
            source_kind: installation.source_kind.clone(),
            action: action.to_owned(),
            decision: "accepted".to_owned(),
            reason_code: None,
            details_json: format!(r#"{{"fingerprint":"{fingerprint}"}}"#),
            created_at_unix: timestamp,
        };

        let conflicted_install_upload = "installconflictupload1";
        insert_upload(&db, &upload(conflicted_install_upload))
            .await
            .expect("conflicted install upload should insert");
        transition_upload(
            &db,
            conflicted_install_upload,
            &["finalized"],
            "aborted",
            None,
            None,
            Some(12),
            12,
        )
        .await
        .expect("conflicted install upload should abort");
        assert!(
            !skill_lifecycle::install(
                &db,
                &installation,
                &policy,
                &[audit("install", "initial", 13)],
                Some(conflicted_install_upload),
                None,
                13,
            )
            .await
            .expect("conflicted install transaction should roll back")
        );
        assert!(
            find_installation(&db, &installation.skill_id)
                .await
                .expect("rolled-back installation lookup should succeed")
                .is_none()
        );
        assert!(
            find_policy(&db, "workspace-one", &installation.skill_id)
                .await
                .expect("rolled-back policy lookup should succeed")
                .is_none()
        );

        let install_upload = "installsuccessupload01";
        insert_upload(&db, &upload(install_upload))
            .await
            .expect("install upload should insert");
        assert!(
            skill_lifecycle::install(
                &db,
                &installation,
                &policy,
                &[audit("install", "initial", 20)],
                Some(install_upload),
                None,
                20,
            )
            .await
            .expect("install lifecycle transaction should succeed")
        );
        assert_eq!(
            find_upload(&db, install_upload)
                .await
                .expect("install upload should query")
                .expect("install upload should remain")
                .status,
            "consumed"
        );

        let update_upload = "updatesuccessupload001";
        insert_upload(&db, &upload(update_upload))
            .await
            .expect("update upload should insert");
        assert!(
            skill_lifecycle::update(
                &db,
                &installation.skill_id,
                &SkillInstallationPatch {
                    fingerprint: Some("updated".to_owned()),
                    ..Default::default()
                },
                &[audit("update", "updated", 30)],
                Some(update_upload),
                None,
                None,
                30,
            )
            .await
            .expect("update lifecycle transaction should succeed")
        );
        assert_eq!(
            find_installation(&db, &installation.skill_id)
                .await
                .expect("updated installation should query")
                .expect("updated installation should remain")
                .fingerprint,
            "updated"
        );

        let conflicted_update_upload = "updateconflictupload01";
        insert_upload(&db, &upload(conflicted_update_upload))
            .await
            .expect("conflicted update upload should insert");
        transition_upload(
            &db,
            conflicted_update_upload,
            &["finalized"],
            "aborted",
            None,
            None,
            Some(31),
            31,
        )
        .await
        .expect("conflicted update upload should abort");
        assert!(
            !skill_lifecycle::update(
                &db,
                &installation.skill_id,
                &SkillInstallationPatch {
                    fingerprint: Some("must-not-commit".to_owned()),
                    ..Default::default()
                },
                &[audit("update", "must-not-commit", 32)],
                Some(conflicted_update_upload),
                None,
                None,
                32,
            )
            .await
            .expect("conflicted update transaction should roll back")
        );
        assert_eq!(
            find_installation(&db, &installation.skill_id)
                .await
                .expect("rolled-back update should query")
                .expect("installation should remain after conflict")
                .fingerprint,
            "updated"
        );
        assert_eq!(
            skill_audit_event::list_skill_audit_events(&db, &installation.skill_id, 16)
                .await
                .expect("lifecycle audit events should query")
                .len(),
            2
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn skill_installation_mutations_target_exact_id_with_duplicate_metadata() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;

        let first = SkillInstallationRecord {
            skill_id: SkillId::new("A".repeat(21)).expect("valid skill id"),
            owner: Some("pioneer".to_owned()),
            slug: "agent-browser".to_owned(),
            version: Some("1.0.0".to_owned()),
            source_kind: "registry".to_owned(),
            scope_key: "ws_one".to_owned(),
            source_ref: "github.com/example/agent-browser".to_owned(),
            install_path: "/tmp/skills/pioneer/agent-browser".to_owned(),
            trust_level: "verified".to_owned(),
            fingerprint: "fp-1".to_owned(),
            updated_at_unix: 1_700_000_000,
            pack_id: None,
            pack_member_key: None,
        };
        skill_installation::insert_standalone(&db, &first, unix_to_datetime(1_700_000_000))
            .await
            .expect("first insert");

        let second = SkillInstallationRecord {
            skill_id: SkillId::new("B".repeat(21)).expect("valid skill id"),
            ..first.clone()
        };
        skill_installation::insert_standalone(&db, &second, unix_to_datetime(1_700_000_100))
            .await
            .expect("duplicate-metadata insert");

        let rows = skill_installation::list_skill_installations(&db)
            .await
            .expect("list skill installations");
        assert_eq!(rows.len(), 2);

        let patch = SkillInstallationPatch {
            version: Some(Some("1.1.0".to_owned())),
            fingerprint: Some("fp-updated".to_owned()),
            ..Default::default()
        };
        assert!(
            skill_reconciliation::update_skill_installation(
                &db,
                &first.skill_id,
                &patch,
                1_700_000_200
            )
            .await
            .expect("exact update")
        );

        let updated = find_installation(&db, &first.skill_id)
            .await
            .expect("find updated row")
            .expect("updated row exists");
        let untouched = find_installation(&db, &second.skill_id)
            .await
            .expect("find duplicate row")
            .expect("duplicate row exists");
        assert_eq!(updated.fingerprint, "fp-updated");
        assert_eq!(updated.version.as_deref(), Some("1.1.0"));
        assert_eq!(untouched.fingerprint, "fp-1");

        assert!(
            skill_installation::delete_skill_installation(&db, &first.skill_id)
                .await
                .expect("exact delete")
        );
        assert!(
            find_installation(&db, &first.skill_id)
                .await
                .expect("find deleted row")
                .is_none()
        );
        assert!(
            find_installation(&db, &second.skill_id)
                .await
                .expect("find surviving duplicate")
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn insertion_rejects_duplicate_skill_id_without_retargeting_metadata() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;
        let record = SkillInstallationRecord {
            skill_id: SkillId::new("C".repeat(21)).expect("valid skill id"),
            owner: None,
            slug: "same".to_owned(),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: "ws_one".to_owned(),
            source_ref: "upload:first".to_owned(),
            install_path: "/tmp/skills/C/same".to_owned(),
            trust_level: "community".to_owned(),
            fingerprint: "fp".to_owned(),
            updated_at_unix: 1_700_000_000,
            pack_id: None,
            pack_member_key: None,
        };
        skill_installation::insert_standalone(&db, &record, unix_to_datetime(1_700_000_000))
            .await
            .expect("first insert");
        assert!(
            skill_installation::insert_standalone(&db, &record, unix_to_datetime(1_700_000_001))
                .await
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn installation_and_default_policy_are_created_and_removed_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = database(fixture).await?;
        let skill_id = SkillId::new("I".repeat(21)).expect("valid skill id");
        let workspace_id = "ws_atomic_skill";
        let installation = SkillInstallationRecord {
            skill_id: skill_id.clone(),
            owner: Some("pioneer".to_owned()),
            slug: "atomic-skill".to_owned(),
            version: None,
            source_kind: "user".to_owned(),
            scope_key: workspace_id.to_owned(),
            source_ref: "upload:atomic".to_owned(),
            install_path: "/tmp/skills/atomic-skill".to_owned(),
            trust_level: "community".to_owned(),
            fingerprint: "atomic-fingerprint".to_owned(),
            updated_at_unix: 1_700_000_000,
            pack_id: None,
            pack_member_key: None,
        };
        let policy = WorkspaceSkillPolicyRecord {
            id: generate_id(21),
            workspace_id: workspace_id.to_owned(),
            skill_id: skill_id.clone(),
            enabled: Some(true),
            allow_implicit_invocation: Some(false),
        };

        skill_lifecycle::insert_with_policy(&db, &installation, &policy, 1_700_000_000)
            .await
            .expect("atomic install persistence");
        assert!(
            find_installation(&db, &skill_id)
                .await
                .expect("find installation")
                .is_some()
        );
        assert_eq!(
            find_policy(&db, workspace_id, &skill_id)
                .await
                .expect("find policy"),
            Some(policy)
        );

        assert!(
            skill_lifecycle::delete_with_workspace_policy(&db, workspace_id, &skill_id)
                .await
                .expect("atomic uninstall persistence")
        );
        assert!(
            find_installation(&db, &skill_id)
                .await
                .expect("find deleted installation")
                .is_none()
        );
        assert!(
            find_policy(&db, workspace_id, &skill_id)
                .await
                .expect("find deleted policy")
                .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}
