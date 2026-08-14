use anyhow::{Context, Result, bail};
use pioneer_crud::CrudStore;
use pioneer_protocol::{PrincipalId, TurnStatus};
use tracing::{info, warn};

const SCAN_BATCH: u64 = 10_000;
const QUARANTINE_REASON: &str =
    "execution quarantined: mandatory authority envelope is missing or invalid";

/// Quarantines incomplete active executions without deriving authority from
/// actor kind, role, or historical row shape. A later explicit restart must
/// create a fresh admission; this worker never synthesizes a grant.
pub(super) async fn run(store: &CrudStore) -> Result<()> {
    run_with_batch_size(store, SCAN_BATCH).await.map(|_| ())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct IntegrityScanSummary {
    scanned: u64,
    quarantined: u64,
}

async fn run_with_batch_size(store: &CrudStore, batch_size: u64) -> Result<IntegrityScanSummary> {
    if batch_size == 0 {
        bail!("execution authority integrity scan requires a non-zero batch size");
    }
    let mut quarantined = 0_u64;
    let mut scanned = 0_u64;
    let mut after_turn_id = None;
    loop {
        let records = store
            .list_active_execution_authorities(after_turn_id.as_deref(), batch_size)
            .await
            .context("failed to read active execution authority records")?;
        let Some(next_cursor) = records.last().map(|record| record.turn_id.clone()) else {
            break;
        };
        let page_len = records.len();
        scanned = scanned.saturating_add(u64::try_from(page_len).unwrap_or(u64::MAX));
        for record in records {
            let validation = validate_record(store, &record).await;
            if validation.is_ok() {
                continue;
            }
            let validation_error = validation.expect_err("invalid record must have an error");
            let updated = store
                .update_turn_status(
                    record.thread_id.as_str(),
                    record.turn_id.as_str(),
                    TurnStatus::Blocked,
                    Some(QUARANTINE_REASON),
                    chrono::Utc::now().timestamp(),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to quarantine execution `{}` with invalid authority",
                        record.turn_id
                    )
                })?;
            if !updated {
                bail!(
                    "active execution `{}` disappeared while quarantining invalid authority",
                    record.turn_id
                );
            }
            quarantined = quarantined.saturating_add(1);
            warn!(
                turn_id = record.turn_id,
                thread_id = record.thread_id,
                error = %format!("{validation_error:#}"),
                "active execution quarantined without synthesizing authority"
            );
        }
        after_turn_id = Some(next_cursor);
        if page_len < usize::try_from(batch_size).unwrap_or(usize::MAX) {
            break;
        }
    }

    let mut after_task_id = None;
    loop {
        let records = store
            .list_active_task_execution_authorities(after_task_id.as_deref(), batch_size)
            .await
            .context("failed to read active Task execution authority records")?;
        let Some(next_cursor) = records.last().map(|record| record.task_id.clone()) else {
            break;
        };
        let page_len = records.len();
        scanned = scanned.saturating_add(u64::try_from(page_len).unwrap_or(u64::MAX));
        for record in records {
            let validation = validate_task_record(&record);
            if validation.is_ok() {
                continue;
            }
            let validation_error = validation.expect_err("invalid record must have an error");
            let blocked_at = chrono::Utc::now().timestamp();
            store
                .append_task_event(
                    pioneer_protocol::TaskEventPayload::TaskBlocked {
                        task_id: record.task_id.clone(),
                        error: Some(pioneer_protocol::TaskError {
                            code: "execution_authority_integrity".to_owned(),
                            message: QUARANTINE_REASON.to_owned(),
                            class: pioneer_protocol::TaskErrorClass::Policy,
                            details: None,
                            failed_run_id: None,
                        }),
                        blocked_at,
                    },
                    blocked_at,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to quarantine Task `{}` with invalid authority",
                        record.task_id
                    )
                })?;
            quarantined = quarantined.saturating_add(1);
            warn!(
                task_id = record.task_id,
                error = %format!("{validation_error:#}"),
                "active Agent Task quarantined without synthesizing authority"
            );
        }
        after_task_id = Some(next_cursor);
        if page_len < usize::try_from(batch_size).unwrap_or(usize::MAX) {
            break;
        }
    }
    info!(
        scanned,
        quarantined, "execution authority integrity scan completed"
    );
    Ok(IntegrityScanSummary {
        scanned,
        quarantined,
    })
}

async fn validate_record(
    store: &CrudStore,
    record: &pioneer_crud::ActiveExecutionAuthorityRecord,
) -> Result<()> {
    record
        .authority_envelope_json
        .as_deref()
        .context("active execution has no authority envelope")?;
    crate::authorization::ExecutionAuthorizationContext::load_for_turn(
        store,
        record.turn_id.as_str(),
    )
    .await
    .map(|_| ())
    .context("active execution durable authority binding is invalid")
}

fn validate_task_record(record: &pioneer_crud::ActiveTaskExecutionAuthorityRecord) -> Result<()> {
    let admission_workspace_id = record
        .admission_workspace_id
        .as_deref()
        .context("active Agent Task has no execution admission")?;
    let root_thread_id = record
        .root_thread_id
        .as_deref()
        .context("active Agent Task admission has no root thread")?;
    let initiating_principal_id = PrincipalId::new(
        record
            .initiating_principal_id
            .as_deref()
            .context("active Agent Task admission has no initiating principal")?,
    )
    .context("active Agent Task initiating principal id is invalid")?;
    let encoded = record
        .authority_envelope_json
        .as_deref()
        .context("active Agent Task has no authority envelope")?;
    let context = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(encoded)
        .context("active Agent Task authority envelope is invalid")?;

    let persisted_principal = match (
        record.principal_id.as_deref(),
        record.principal_kind.as_deref(),
    ) {
        (Some(id), Some(kind)) => Some((
            PrincipalId::new(id).context("active Agent Task principal id is invalid")?,
            pioneer_crud::principal_kind_from_db(kind)
                .context("active Agent Task principal kind is invalid")?,
        )),
        (None, None) => None,
        _ => bail!("active Agent Task has an incomplete principal row"),
    };
    let admission_actor =
        pioneer_protocol::PersistedActorRef::Principal(initiating_principal_id.clone());
    context.verify_persisted_actor_binding(
        Some(&admission_actor),
        persisted_principal
            .as_ref()
            .map(|(principal_id, principal_kind)| (principal_id, *principal_kind)),
    )?;

    if record.task_workspace_id != admission_workspace_id
        || context.workspace_id() != admission_workspace_id
        || context.root_thread_id() != root_thread_id
        || context.initiating_principal_id() != &initiating_principal_id
    {
        bail!("active Agent Task differs from its immutable execution admission");
    }
    if record.root_thread_workspace_id.as_deref() != Some(admission_workspace_id) {
        bail!("active Agent Task execution root is missing or belongs to another workspace");
    }
    let root_access_class = record
        .root_thread_access_class
        .as_deref()
        .context("active Agent Task execution root has no access class")
        .and_then(pioneer_crud::persisted_thread_access_class_from_db)?;
    if root_access_class == pioneer_crud::PersistedThreadAccessClass::Internal {
        bail!("active Agent Task execution admission is rooted in an internal child");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::CrudStore;
    use pioneer_protocol::{
        SandboxMode, TaskStatus, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnStatus, default_turn_permission_profile_snapshot,
    };
    use sea_orm::{Database, EntityTrait, Set};

    use super::{QUARANTINE_REASON, run_with_batch_size};

    #[tokio::test]
    async fn startup_gate_scans_every_page_and_quarantines_every_invalid_binding() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let now = chrono::Utc::now().fixed_offset();
        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set("workspace_authority_integrity".to_owned()),
            name: Set("Authority integrity".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("workspace should insert");

        let store = CrudStore::new(connection);
        crate::session::test_support::ensure_test_superuser_execution_authority(&store).await;
        let principal = crate::session::test_support::authenticated_test_superuser();
        let thread_id = "thread_authority_integrity";
        let thread = Thread {
            workspace_id: "workspace_authority_integrity".to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "test-provider".to_owned(),
            reasoning_effort: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        for turn_id in [
            "turn-authority-a",
            "turn-authority-b",
            "turn-authority-c",
            "turn-authority-d",
            "turn-authority-e",
            "turn-authority-f",
            "turn-authority-g",
            "turn-authority-h",
        ] {
            store
                .materialize_turn_start(
                    &thread,
                    SandboxMode::FullAccess,
                    &Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::InProgress,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        mode: Default::default(),
                        author: None,
                        reply_to_turn_id: None,
                        mentions: Vec::new(),
                        message_revision: 0,
                        message_deleted: false,
                        error: None,
                        prompt_manifest: None,
                        permission_profile: default_turn_permission_profile_snapshot(),
                    },
                    &[],
                    pioneer_protocol::PersistedActorRef::Principal(principal.principal_id.clone()),
                )
                .await
                .expect("turn should materialize");
        }

        let context = crate::authorization::ExecutionAuthorizationContext::for_test(
            principal.as_ref(),
            "workspace_authority_integrity",
            thread_id,
            &default_turn_permission_profile_snapshot(),
            None,
        );
        let valid_json = context.to_persisted_json().expect("valid authority JSON");
        assert!(
            store
                .set_turn_execution_authorization_context("turn-authority-a", &valid_json)
                .await
                .expect("valid authority should persist")
        );
        assert!(
            store
                .set_turn_execution_authorization_context("turn-authority-c", "{not-json")
                .await
                .expect("corrupt authority should persist for integrity test")
        );
        let mut unsupported: serde_json::Value =
            serde_json::from_str(&valid_json).expect("valid authority value");
        unsupported["authority"] = serde_json::json!({
            "kind": "system_grant",
            "issuer": "unregistered-startup-producer",
            "policy_generation": 1
        });
        assert!(
            store
                .set_turn_execution_authorization_context(
                    "turn-authority-d",
                    &serde_json::to_string(&unsupported).expect("unsupported authority JSON"),
                )
                .await
                .expect("unsupported authority should persist for integrity test")
        );
        assert!(
            store
                .set_turn_execution_authorization_context("turn-authority-e", &valid_json)
                .await
                .expect("missing-actor authority should persist for integrity test")
        );

        let mut dangling_principal = principal.as_ref().clone();
        dangling_principal.principal_id =
            pioneer_protocol::PrincipalId::new("P00000000000000000002")
                .expect("dangling principal id should be valid");
        let dangling_context = crate::authorization::ExecutionAuthorizationContext::for_test(
            &dangling_principal,
            "workspace_authority_integrity",
            thread_id,
            &default_turn_permission_profile_snapshot(),
            None,
        )
        .to_persisted_json()
        .expect("dangling principal context should encode");
        assert!(
            store
                .set_turn_execution_authorization_context(
                    "turn-authority-f",
                    dangling_context.as_str(),
                )
                .await
                .expect("dangling-principal authority should persist for integrity test")
        );

        let wrong_root_context = crate::authorization::ExecutionAuthorizationContext::for_test(
            principal.as_ref(),
            "workspace_authority_integrity",
            "thread_authority_other_root",
            &default_turn_permission_profile_snapshot(),
            None,
        )
        .to_persisted_json()
        .expect("wrong-root context should encode");
        assert!(
            store
                .set_turn_execution_authorization_context(
                    "turn-authority-g",
                    wrong_root_context.as_str(),
                )
                .await
                .expect("wrong-root authority should persist for integrity test")
        );
        assert!(
            store
                .set_turn_execution_authorization_context("turn-authority-h", &valid_json)
                .await
                .expect("unknown-actor authority should persist for integrity test")
        );

        use sea_orm::ConnectionTrait;
        store
            .database_connection()
            .execute_unprepared(
                "UPDATE \"turn\" SET initiated_by_actor_kind=NULL, initiated_by_actor_id=NULL \
                 WHERE id='turn-authority-e'; \
                 UPDATE \"turn\" SET initiated_by_actor_kind='principal', \
                     initiated_by_actor_id='P00000000000000000002' \
                 WHERE id='turn-authority-f'; \
                 UPDATE \"turn\" SET initiated_by_actor_kind='system', \
                     initiated_by_actor_id=NULL \
                 WHERE id='turn-authority-h';",
            )
            .await
            .expect("invalid actor fixtures should persist");

        store
            .database_connection()
            .execute_unprepared(
                "INSERT INTO task(\
                    id,workspace_id,owner_kind,owner_id,created_by_thread_id,\
                    executor_kind,status,title,goal,priority,revision,created_at,updated_at\
                 ) VALUES\
                    ('task-authority-a','workspace_authority_integrity','user',\
                     'P00000000000000000001','thread_authority_integrity','agent','waiting',\
                     'Valid authority','Valid authority',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('task-authority-b','workspace_authority_integrity','user',\
                     'P00000000000000000001','thread_authority_integrity','agent','waiting',\
                     'Missing authority','Missing authority',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('task-authority-c','workspace_authority_integrity','user',\
                     'P00000000000000000001','thread_authority_integrity','agent','waiting',\
                     'Corrupt authority','Corrupt authority',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                    ('task-authority-d','workspace_authority_integrity','user',\
                     'P00000000000000000002','thread_authority_integrity','agent','waiting',\
                     'Dangling authority','Dangling authority',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);",
            )
            .await
            .expect("Agent Task integrity fixtures should persist");
        for (task_id, principal_id, authority_json) in [
            (
                "task-authority-a",
                principal.principal_id.as_str(),
                valid_json.as_str(),
            ),
            (
                "task-authority-c",
                principal.principal_id.as_str(),
                "{not-json",
            ),
            (
                "task-authority-d",
                dangling_principal.principal_id.as_str(),
                dangling_context.as_str(),
            ),
        ] {
            pioneer_entity::task_execution_admission::Entity::insert(
                pioneer_entity::task_execution_admission::ActiveModel {
                    task_id: Set(task_id.to_owned()),
                    workspace_id: Set("workspace_authority_integrity".to_owned()),
                    root_thread_id: Set(thread_id.to_owned()),
                    initiating_principal_id: Set(principal_id.to_owned()),
                    authorization_context_json: Set(authority_json.to_owned()),
                    created_at: Set(now),
                },
            )
            .exec(&store.database_connection())
            .await
            .expect("Task execution admission fixture should persist");
        }

        // A batch size of two forces the test to cross the same keyset boundary
        // that the production 10k-page scan uses.
        let summary = run_with_batch_size(&store, 2)
            .await
            .expect("integrity gate should complete");
        assert_eq!(summary.scanned, 12);
        assert_eq!(summary.quarantined, 10);

        let valid = store
            .get_turn(thread_id, "turn-authority-a")
            .await
            .expect("valid turn lookup")
            .expect("valid turn")
            .1;
        assert_eq!(valid.status, TurnStatus::InProgress);
        assert!(valid.error.is_none());
        for turn_id in [
            "turn-authority-b",
            "turn-authority-c",
            "turn-authority-d",
            "turn-authority-e",
            "turn-authority-f",
            "turn-authority-g",
            "turn-authority-h",
        ] {
            let quarantined = store
                .get_turn(thread_id, turn_id)
                .await
                .expect("quarantined turn lookup")
                .expect("quarantined turn")
                .1;
            assert_eq!(quarantined.status, TurnStatus::Blocked, "{turn_id}");
            assert_eq!(quarantined.error.as_deref(), Some(QUARANTINE_REASON));
        }

        let valid_task = store
            .get_task("task-authority-a")
            .await
            .expect("valid Task lookup")
            .expect("valid Task");
        assert_eq!(valid_task.task.status, TaskStatus::Waiting);
        for task_id in ["task-authority-b", "task-authority-c", "task-authority-d"] {
            let quarantined = store
                .get_task(task_id)
                .await
                .expect("quarantined Task lookup")
                .expect("quarantined Task");
            assert_eq!(quarantined.task.status, TaskStatus::Blocked, "{task_id}");
            assert_eq!(
                quarantined
                    .task
                    .error
                    .as_ref()
                    .map(|error| error.code.as_str()),
                Some("execution_authority_integrity")
            );
        }
    }
}
