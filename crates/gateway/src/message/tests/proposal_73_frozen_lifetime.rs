//! Future production-entry fixtures. Never run/compiled before independent review.
use super::*;
use pioneer_compaction::{
    CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, ModelBudget,
    ModelSelection, OperationSnapshot, SourceRef, Transport,
};
use pioneer_crud::compaction::{ManifestEntry, PagedSource, frozen_import_identity};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use sha2::{Digest, Sha256};

struct Fits;
#[async_trait::async_trait]
impl crate::compaction::CompactionTarget for Fits {
    async fn fits(&self, _: &str) -> anyhow::Result<bool> {
        Ok(true)
    }
}
struct Silent;
#[async_trait::async_trait]
impl crate::compaction::CompactionObserver for Silent {
    async fn started(
        &self,
        _: &str,
        _: &pioneer_compaction::runner::RunnerState,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn heartbeat(&self, _: &str) {}
    async fn terminal(
        &self,
        _: &str,
        _: &pioneer_compaction::runner::RunnerState,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
struct FrozenFixture {
    _directory: tempfile::TempDir,
    h: Phase13CompactionHarness,
    origin: pioneer_compaction::frozen::FrozenHistoryRef,
    checkpoint: String,
    output: pioneer_crud::compaction::TaskOutputSnapshot,
}
async fn materialize(
    h: &Phase13CompactionHarness,
    thread: &str,
    turn: &str,
    text: &str,
    completed: bool,
) {
    ensure_test_superuser_execution_authority(&h.crud_store).await;
    let timestamp = phase_13_now_secs();
    h.crud_store
        .materialize_turn_start(
            &phase_13_test_thread(&h.workspace_id, thread, timestamp),
            SandboxMode::FullAccess,
            &phase_13_turn(turn, TurnStatus::InProgress),
            &[UserInput::Text {
                text: format!("input {text}"),
                text_elements: vec![],
            }],
            pioneer_protocol::PersistedActorRef::Principal(
                authenticated_test_superuser().principal_id.clone(),
            ),
        )
        .await
        .unwrap();
    if completed {
        h.crud_store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: h.workspace_id.clone(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item: TurnItem::AgentMessage {
                        id: format!("{turn}-answer"),
                        text: text.into(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                timestamp + 1,
            )
            .await
            .unwrap();
        h.crud_store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: h.workspace_id.clone(),
                    thread_id: thread.into(),
                    turn: phase_13_turn(turn, TurnStatus::Completed),
                },
                timestamp + 2,
            )
            .await
            .unwrap();
    }
}
async fn ack(h: &Phase13CompactionHarness, delivery: &str, thread: &str, turn: &str) -> SourceRef {
    h.crud_store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: h.workspace_id.clone(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                item: TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id(delivery),
                    text: "retained delivered work".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            phase_13_now_secs() + 3,
        )
        .await
        .unwrap();
    h.crud_store
        .compaction_source_page(&h.workspace_id, thread, turn, PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|row| {
            row.item_id.as_deref()
                == Some(pioneer_protocol::task_delivery_result_item_id(delivery).as_str())
        })
        .unwrap()
        .reference
}
async fn delivery(h: &Phase13CompactionHarness, id: &str, thread: &str, turn: &str) -> SourceRef {
    let db = h.crud_store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES(?1,?2,'p73-task','p73-run',?1,'thread','origin_thread',?3,'delivered',1,1,?4)",[id.into(),h.workspace_id.clone().into(),thread.into(),turn.into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES(?1,'p73-candidate','p73-rt')",[id.into()])).await.unwrap();
    ack(h, id, thread, turn).await
}
#[derive(Clone, Copy)]
enum CorrectionFixture {
    Normal,
    WorkerLegacy,
    Aggregate { legacy: bool },
    UnderstatedOutput { shared: bool },
}
async fn fixture() -> FrozenFixture {
    fixture_with_correction(CorrectionFixture::Normal).await
}
async fn fixture_with_correction(mode: CorrectionFixture) -> FrozenFixture {
    let provider = Arc::new(
        CaptureSummaryProvider::new("unused")
            .with_valid_summary_completion()
            .with_summary_marker("P73 PERMANENT SUMMARY"),
    );
    let (directory, workspace_manager, crud_store, workspace_id) =
        setup_pooled_file_workspace_manager().await;
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        phase_13_provider_registry(provider.clone()),
        Arc::new(SessionManager::new()),
        workspace_manager,
        crud_store.clone(),
        test_gateway_secrets(),
        phase_13_summary_config(),
        test_tool_loop_config(),
    ));
    let h = Phase13CompactionHarness {
        processor,
        crud_store,
        workspace_id,
    };
    let tx = h.crud_store.database_connection().begin().await.unwrap();
    let pragma: i64 = tx
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "foreign_keys")
        .unwrap();
    assert_eq!(
        pragma, 0,
        "lifetime/proof fixtures use the production writer FK policy"
    );
    tx.commit().await.unwrap();
    materialize(
        &h,
        "p73-source",
        "p73-source-turn",
        "retained delivered work",
        true,
    )
    .await;
    let db = h.crud_store.database_connection();
    for (sql, values) in [
        (
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('p73-task',?1,'thread','p73-source','p73-source','p73-source-turn','agent','completed','fixture','fixture')",
            vec![h.workspace_id.clone().into()],
        ),
        (
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('p73-run','p73-task','p73-run',1,1,'succeeded','agent')",
            vec![],
        ),
        (
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('p73-rt','p73-task','p73-run','p73-source','p73-source-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
            vec![],
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .unwrap();
    }
    let rt = h
        .crud_store
        .get_task_run_turn("p73-rt")
        .await
        .unwrap()
        .unwrap();
    let output =
        crate::compaction::frozen::capture_task_output(&h.crud_store, &h.workspace_id, &rt)
            .await
            .unwrap();
    db.execute_unprepared("INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES('p73-candidate','p73-task','p73-run','p73-rt','p73-source','p73-source-turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let original_ack = delivery(&h, "p73-original", "p73-source", "p73-source-turn").await;
    let mut refs = h
        .crud_store
        .compaction_frozen_history_page(
            &h.workspace_id,
            "p73-source",
            &output.history.manifest_id,
            0,
        )
        .await
        .unwrap();
    assert!(!refs.is_empty());
    let mut imports = vec![];
    for (ordinal, target) in refs.iter().enumerate() {
        for source in &target.sources {
            let prepared = h
                .crud_store
                .compaction_prepare_frozen_import(
                    &h.workspace_id,
                    "p73-source",
                    "p73-original",
                    &original_ack,
                    ordinal as u64,
                    "p73-source",
                    source,
                )
                .await
                .unwrap();
            imports.push((ordinal as u64, prepared));
        }
    }
    assert!(!imports.is_empty());
    if matches!(mode, CorrectionFixture::Aggregate { .. }) {
        // A persisted legacy selected set may be larger than one metadata page.
        // Keep each source/reference small and the original accepted imports
        // nonempty. Identical canonical event wire is owned by distinct IDs.
        let exemplar = refs
            .iter()
            .find(|r| {
                r.event_input_role
                    == Some(pioneer_compaction::frozen::FrozenEventInputRole::Authoritative)
            })
            .unwrap()
            .clone();
        let event = exemplar
            .sources
            .iter()
            .find(|s| s.scope.starts_with("event:"))
            .unwrap();
        let canonical = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT event_type,payload FROM turn_event WHERE id=?1",
                [event.id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap();
        let event_type: String = canonical.try_get("", "event_type").unwrap();
        let payload: String = canonical.try_get("", "payload").unwrap();
        for i in 0..128 {
            let id = format!("p73-page-{i:04}-{}", "x".repeat(3072));
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES(?1,'p73-source','p73-source-turn',?2,?3,?4,CURRENT_TIMESTAMP)",
                [id.clone().into(),(10_000_i64+i).into(),event_type.clone().into(),payload.clone().into()])).await.unwrap();
            let mut reference = exemplar.clone();
            reference.unit_id = format!("page-unit-{i}");
            reference.sources = vec![SourceRef {
                scope: event.scope.clone(),
                id,
                version: "event-revision:1".into(),
            }];
            reference.source_aliases.clear();
            reference.ambiguous_input_aliases.clear();
            reference.publication_aliases = None;
            reference.replay_source = None;
            reference.tool_item_id = None;
            refs.push(reference);
        }
        assert!(
            refs.iter()
                .flat_map(|r| &r.sources)
                .map(|s| s.scope.len() + s.id.len() + s.version.len())
                .sum::<usize>()
                > pioneer_crud::compaction::SOURCE_PAGE_BYTES
        );
    }
    if matches!(mode, CorrectionFixture::WorkerLegacy) {
        // Automatic conversion is active too. Give the origin a distinct first
        // unit (before computing its identity) so the retained output cannot
        // legitimately borrow this origin's physical prefix during the test.
        refs[0].unit_id = format!("worker-origin:{}", refs[0].unit_id);
    }
    let mut digest = Sha256::new();
    for reference in &refs {
        let bytes = serde_json::to_vec(reference).unwrap();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    let origin = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "p73-origin".into(),
        identity_sha256: hex::encode(digest.finalize()),
        messages: refs.len() as u64,
    };
    h.crud_store
        .compaction_begin_frozen_history_with_imports(
            &h.workspace_id,
            "p73-source",
            &origin,
            imports.len() as u64,
            &frozen_import_identity(&imports).unwrap(),
        )
        .await
        .unwrap();
    for (page, records) in refs.chunks(16).enumerate() {
        h.crud_store
            .compaction_append_frozen_history(
                &h.workspace_id,
                "p73-source",
                &origin.manifest_id,
                (page * 16) as u64,
                records,
            )
            .await
            .unwrap();
    }
    h.crud_store
        .compaction_append_frozen_imports(
            &h.workspace_id,
            "p73-source",
            &origin.manifest_id,
            0,
            &imports,
        )
        .await
        .unwrap();
    let hold = h
        .crud_store
        .compaction_finish_frozen_history_held(&h.workspace_id, "p73-source", &origin)
        .await
        .unwrap();
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "summary-capture".into(),
        model: "test-model".into(),
        effort: None,
    };
    let now = chrono::Utc::now().timestamp_millis() as u64;
    let snapshot = OperationSnapshot {
        id: "p73-operation".into(),
        owner: crate::compaction::native_owner(&h.workspace_id, "p73-source"),
        expected_checkpoint: None,
        projection_version: h
            .crud_store
            .compaction_projection_version(&h.workspace_id, "p73-source")
            .await
            .unwrap(),
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(&selection, None, now)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::WorkingContext,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: "p73-origin".into(),
        },
    };
    let budget = ModelBudget::new(Some(65_536), None, None);
    h.crud_store
        .compaction_admit_for_turn(
            &h.workspace_id,
            "p73-source",
            &snapshot,
            Some("p73-source-turn"),
        )
        .await
        .unwrap();
    h.crud_store
        .compaction_bind_source_projection(&snapshot.id, &origin)
        .await
        .unwrap();
    let manifest: Vec<_> = refs
        .iter()
        .enumerate()
        .flat_map(|(unit, r)| r.sources.iter().map(move |s| (unit, s.clone())))
        .enumerate()
        .map(|(ordinal, (unit, source))| ManifestEntry {
            ordinal: ordinal as u64,
            unit: unit as u64,
            reference_only: false,
            thread_id: "p73-source".into(),
            source,
        })
        .collect();
    h.crud_store
        .compaction_prepare_runner(&snapshot.id, &budget, manifest.len() as u64, 0)
        .await
        .unwrap();
    for records in manifest.chunks(16) {
        h.crud_store
            .compaction_append_manifest(&snapshot.id, records)
            .await
            .unwrap();
    }
    h.crud_store
        .compaction_activate_runner(
            &snapshot.id,
            &pioneer_compaction::runner::RunnerState::new(
                snapshot.admission.deadline_ms,
                &budget,
                512,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let checkpoint = if matches!(mode, CorrectionFixture::Normal) {
        let summarizer = Arc::new(
            pioneer_agent::compaction::NativeSummarizer::new(provider, selection, budget).unwrap(),
        );
        let runner = crate::compaction::CompactionRunner::new(
            h.crud_store.as_ref().clone(),
            h.workspace_id.clone(),
            "p73-source".into(),
            snapshot,
            summarizer,
            Arc::new(Fits),
            Arc::new(Silent),
            Arc::new(crate::compaction::SystemCompactionClock::default()),
        );
        let crate::compaction::CompactionExit::Applied(checkpoint) = runner
            .run(tokio_util::sync::CancellationToken::new())
            .await
            .unwrap()
        else {
            panic!("fixture checkpoint must publish")
        };
        checkpoint
    } else {
        // Exercise existing candidate + atomic publication entry points with a
        // saved summary; provider/tokenization cost is outside these fixtures.
        let mut state = h
            .crud_store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        let cp = pioneer_compaction::Checkpoint {
            id: "p73-correction-checkpoint".into(),
            operation_id: snapshot.id.clone(),
            owner: snapshot.owner.clone(),
            previous: None,
            summary: "P73 PERMANENT SUMMARY".into(),
            selection: snapshot.admission.selection.clone(),
            coverage: manifest
                .iter()
                .map(|m| m.source.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            projection_version: snapshot.projection_version,
            format_version: 1,
        };
        state.generation += 1;
        state.attempts = 1;
        state.previous_checkpoint = Some(cp.id.clone());
        state.cursor = pioneer_compaction::runner::SourceCursor {
            unit: refs.len() as u64,
            ..Default::default()
        };
        state.source_text_projection_version = 1;
        state.phase = pioneer_compaction::runner::RunnerPhase::Candidate {
            checkpoint: cp.id.clone(),
            final_portion: true,
        };
        assert!(
            h.crud_store
                .compaction_runner_transition(&snapshot.id, 0, &state, Some(&cp))
                .await
                .unwrap()
        );
        let commit = state.candidate_checked(true).unwrap();
        assert!(
            h.crud_store
                .compaction_runner_transition(&snapshot.id, state.generation, &commit, None)
                .await
                .unwrap()
        );
        // Legacy extractor must already traverse the valid nonempty import set.
        let legacy = h
            .crud_store
            .compaction_checkpoint_edges(&cp.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.coverage.len(), cp.coverage.len());
        if let CorrectionFixture::UnderstatedOutput { shared } = mode {
            let backing = if shared {
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,import_count,imports_sha256,ready,next_ordinal,next_import) SELECT 'p73-output-backing',workspace_id,owner_thread,identity_sha256,message_count,0,?2,ready,next_ordinal,0 FROM compaction_frozen_history WHERE id=?1",
                    [output.history.manifest_id.clone().into(),hex::encode(Sha256::digest([])).into()])).await.unwrap();
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) SELECT 'p73-output-backing',ordinal,reference_json,bytes FROM compaction_frozen_message_data WHERE manifest_id=?1",
                    [output.history.manifest_id.clone().into()])).await.unwrap();
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES(?1,0,0,1)",[output.history.manifest_id.clone().into()])).await.unwrap();
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES(?1,0,0,?2,'p73-output-backing')",
                    [output.history.manifest_id.clone().into(),(output.history.messages as i64).into()])).await.unwrap();
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "UPDATE compaction_frozen_layout SET active=1,pending=0 WHERE manifest_id=?1 AND kind=0",[output.history.manifest_id.clone().into()])).await.unwrap();
                "p73-output-backing".to_owned()
            } else {
                output.history.manifest_id.clone()
            };
            let mut bad = refs[0].clone();
            bad.unit_id = "x".repeat(pioneer_crud::compaction::SOURCE_PAGE_BYTES + 1);
            let oversized = serde_json::to_string(&bad).unwrap();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_frozen_message_data SET reference_json=?2,bytes=1 WHERE manifest_id=?1 AND ordinal=0",
                [backing.clone().into(),oversized.into()])).await.unwrap();
            let error = h
                .crud_store
                .compaction_apply_runner(&snapshot.id, &commit, None)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("original import target"),
                "must reach retained OUTPUT helper, not reject origin: {error:#}"
            );
            assert_eq!(
                h.crud_store.compaction_head(&snapshot.owner).await.unwrap(),
                None
            );
            let marker: i64 = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT proof_version FROM compaction_checkpoint WHERE id=?1",
                    [cp.id.clone().into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "proof_version")
                .unwrap();
            assert_eq!(marker, 0, "no seal or publication on corrupt output");
            let badrow=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT length(CAST(reference_json AS BLOB)) AS actual,bytes FROM compaction_frozen_message_data WHERE manifest_id=?1 AND ordinal=0",[backing.into()])).await.unwrap().unwrap();
            assert!(
                badrow.try_get::<i64>("", "actual").unwrap()
                    > pioneer_crud::compaction::SOURCE_PAGE_BYTES as i64
            );
            assert_eq!(
                badrow.try_get::<i64>("", "bytes").unwrap(),
                1,
                "refusal preserves physical data"
            );
            assert_eq!(
                h.crud_store
                    .compaction_frozen_history_page(
                        &h.workspace_id,
                        "p73-source",
                        &origin.manifest_id,
                        0
                    )
                    .await
                    .unwrap(),
                refs,
                "origin remains valid; only retained output is corrupt"
            );
            drop(hold);
            drop(imports);
            return FrozenFixture {
                _directory: directory,
                h,
                origin,
                checkpoint: cp.id,
                output,
            };
        }
        if matches!(
            mode,
            CorrectionFixture::Aggregate { legacy: true } | CorrectionFixture::WorkerLegacy
        ) {
            // Persisted pre-upgrade publication, before proof_version existed.
            let applied = commit.applied(&cp.id).unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_runner_state SET generation=?2,state=?3 WHERE operation_id=?1",
                [
                    snapshot.id.clone().into(),
                    (applied.generation as i64).into(),
                    serde_json::to_string(&applied).unwrap().into(),
                ],
            ))
            .await
            .unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_operation SET status='completed' WHERE id=?1",
                [snapshot.id.clone().into()],
            ))
            .await
            .unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_checkpoint SET status='applied' WHERE id=?1",
                [cp.id.clone().into()],
            ))
            .await
            .unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_context SET head=?2 WHERE owner=?1",
                [snapshot.owner.clone().into(), cp.id.clone().into()],
            ))
            .await
            .unwrap();
            if !matches!(mode, CorrectionFixture::WorkerLegacy) {
                let mut progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
                let mut sealed = false;
                for _ in 0..40_000 {
                    progress.quantum(&h.crud_store).await.unwrap();
                    let marker: i64 = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Sqlite,
                            "SELECT proof_version FROM compaction_checkpoint WHERE id=?1",
                            [cp.id.clone().into()],
                        ))
                        .await
                        .unwrap()
                        .unwrap()
                        .try_get("", "proof_version")
                        .unwrap();
                    if marker == 1 {
                        sealed = true;
                        break;
                    }
                }
                assert!(
                    sealed,
                    "legacy large origin must backfill through bounded maintenance steps"
                );
                drop(progress);
            }
        } else {
            assert_eq!(
                h.crud_store
                    .compaction_apply_runner(&snapshot.id, &commit, None)
                    .await
                    .unwrap(),
                pioneer_crud::compaction::CommitOutcome::Applied
            );
        }
        cp.id
    };
    let n: i64 = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint_import WHERE checkpoint_id=?1",
            [checkpoint.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    if matches!(mode, CorrectionFixture::WorkerLegacy) {
        assert_eq!(n, 0, "restart fixture starts before legacy proof backfill");
    } else {
        assert!(n > 0, "A/B must exercise nonempty permanent imports");
    }
    drop(hold);
    drop(imports);
    FrozenFixture {
        _directory: directory,
        h,
        origin,
        checkpoint,
        output,
    }
}
async fn expire_and_sweep(f: &FrozenFixture) {
    let db = f.h.crud_store.database_connection();
    let mut progress = pioneer_crud::FrozenStorageLifetimeProgress::default();
    for _ in 0..20_000 {
        progress.quantum(&f.h.crud_store).await.unwrap();
        let expired: i64 = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT expired FROM compaction_frozen_history WHERE id=?1",
                [f.origin.manifest_id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "expired")
            .unwrap();
        let rows:i64=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT (SELECT count(*) FROM compaction_frozen_message_data WHERE manifest_id=?1)+(SELECT count(*) FROM compaction_frozen_import_data WHERE manifest_id=?1) AS n",[f.origin.manifest_id.clone().into()])).await.unwrap().unwrap().try_get("","n").unwrap();
        if expired == 1 && rows == 0 {
            break;
        }
    }
    drop(progress);
    for table in [
        "compaction_frozen_message_data",
        "compaction_frozen_import_data",
    ] {
        let n: i64 = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("SELECT count(*) AS n FROM {table} WHERE manifest_id=?1"),
                [f.origin.manifest_id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(n, 0, "origin rows really must be absent");
    }
}
#[tokio::test]
async fn published_local_checkpoint_with_nonempty_proofs_survives_expired_origin_through_processor()
{
    let f = fixture().await;
    expire_and_sweep(&f).await;
    materialize(&f.h, "p73-source", "p73-tail", "P73 NEW TAIL", true).await;
    let prepared = capture_current(
        &f,
        "p73-source",
        "p73-tail",
        &authenticated_test_superuser(),
    )
    .await
    .unwrap();
    commit_current(&f, "p73-source", "p73-tail", &prepared).await;
    assert_ne!(prepared.descriptor.manifest_id, f.origin.manifest_id);
    assert!(
        prepared
            .messages
            .iter()
            .any(|m| m.content.contains("P73 PERMANENT SUMMARY"))
    );
    assert!(
        prepared
            .messages
            .iter()
            .any(|m| m.content.contains("P73 NEW TAIL"))
    );
    let refs =
        f.h.crud_store
            .compaction_frozen_history_page(
                &f.h.workspace_id,
                "p73-source",
                &prepared.descriptor.manifest_id,
                0,
            )
            .await
            .unwrap();
    assert!(refs.iter().any(|r| {
        r.sources
            .iter()
            .any(|s| s.scope.starts_with("checkpoint:") && s.id == f.checkpoint)
    }));
    assert!(
        f.h.crud_store
            .compaction_acquire_frozen_history(&f.h.workspace_id, &f.origin)
            .await
            .is_err(),
        "shared bytes or proofs cannot resurrect exact replay"
    );
}
#[tokio::test]
async fn retained_output_checkpoint_replacement_uses_fresh_ordinals_through_processor_after_expiry()
{
    let f = fixture().await;
    materialize(
        &f.h,
        "p73-recipient",
        "p73-recipient-turn",
        "recipient own tail",
        true,
    )
    .await;
    let fresh_ack = delivery(&f.h, "p73-fresh", "p73-recipient", "p73-recipient-turn").await;
    expire_and_sweep(&f).await;
    let prepared = capture_current(
        &f,
        "p73-recipient",
        "p73-recipient-turn",
        &authenticated_test_superuser(),
    )
    .await
    .unwrap();
    commit_current(&f, "p73-recipient", "p73-recipient-turn", &prepared).await;
    let imports =
        f.h.crud_store
            .compaction_frozen_import_page(
                &f.h.workspace_id,
                "p73-recipient",
                &prepared.descriptor.manifest_id,
                0,
            )
            .await
            .unwrap();
    assert!(!imports.is_empty());
    assert!(imports.iter().all(|r| r.delivery_id == "p73-fresh"
        && r.acknowledgement == fresh_ack
        && r.output_manifest == f.output.history.manifest_id));
    let refs =
        f.h.crud_store
            .compaction_frozen_history_page(
                &f.h.workspace_id,
                "p73-recipient",
                &prepared.descriptor.manifest_id,
                0,
            )
            .await
            .unwrap();
    for import in imports {
        let target = &refs[import.message_ordinal as usize];
        assert_eq!(target.sources.len(), 1);
        assert_eq!(target.sources[0].id, f.checkpoint);
        assert!(target.sources[0].scope.starts_with("checkpoint:"));
        assert_eq!(target.context_thread.as_deref(), Some("p73-recipient"));
    }
    assert!(
        prepared
            .messages
            .iter()
            .any(|m| m.content.contains("P73 PERMANENT SUMMARY"))
    );
}

#[tokio::test]
async fn required_expired_task_parent_basis_is_rejected_before_summary_fallback() {
    let f = fixture().await;
    materialize(&f.h, "p73-child", "p73-child-turn", "child", true).await;
    let db = f.h.crud_store.database_connection();
    for (sql, values) in [
        (
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at,origin_kind) VALUES('p73-child','p73-source','p73-source',1,CURRENT_TIMESTAMP,'task_run')",
            vec![],
        ),
        (
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES('p73-child-task',?1,'thread','p73-source','p73-source','p73-source-turn','agent','completed','child','child')",
            vec![f.h.workspace_id.clone().into()],
        ),
        (
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES('p73-child-run','p73-child-task','p73-child-run',1,1,'succeeded','agent')",
            vec![],
        ),
        (
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('p73-child-rt','p73-child-task','p73-child-run','p73-child','p73-child-turn','initial',0,1,'completed',CURRENT_TIMESTAMP)",
            vec![],
        ),
        (
            "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) VALUES('p73-child-run','p73-child-task',?1,'p73-source',?2,CURRENT_TIMESTAMP,?3)",
            vec![
                f.h.workspace_id.clone().into(),
                serde_json::to_string(&f.origin).unwrap().into(),
                f.origin.manifest_id.clone().into(),
            ],
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            values,
        ))
        .await
        .unwrap();
    }
    expire_and_sweep(&f).await;
    let error = capture_current(
        &f,
        "p73-child",
        "p73-child-turn",
        &authenticated_test_superuser(),
    )
    .await
    .err()
    .expect("required exact expired basis must fail");
    assert!(
        format!("{error:#}").contains("expired"),
        "expiry must not become missing/current-history fallback: {error:#}"
    );
}
#[tokio::test]
async fn checkpoint_proofs_do_not_restore_revoked_consumer_read_authority() {
    let f = fixture().await;
    materialize(
        &f.h,
        "p73-recipient",
        "p73-recipient-turn",
        "recipient",
        true,
    )
    .await;
    delivery(&f.h, "p73-fresh", "p73-recipient", "p73-recipient-turn").await;
    expire_and_sweep(&f).await;
    let denied = authenticated_test_member_collaborator();
    assert!(
        capture_current(&f, "p73-recipient", "p73-recipient-turn", &denied)
            .await
            .is_err(),
        "permanent historical evidence cannot grant current thread read"
    );
}
#[tokio::test]
async fn missing_fresh_ack_does_not_forward_permanent_historical_import() {
    let f = fixture().await;
    materialize(
        &f.h,
        "p73-recipient",
        "p73-recipient-turn",
        "recipient",
        true,
    )
    .await;
    let ack = delivery(&f.h, "p73-fresh", "p73-recipient", "p73-recipient-turn").await;
    expire_and_sweep(&f).await;
    f.h.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM turn_event WHERE id=?1",
            [ack.id.into()],
        ))
        .await
        .unwrap();
    let prepared = capture_current(
        &f,
        "p73-recipient",
        "p73-recipient-turn",
        &authenticated_test_superuser(),
    )
    .await
    .unwrap();
    let imports =
        f.h.crud_store
            .compaction_frozen_import_page(
                &f.h.workspace_id,
                "p73-recipient",
                &prepared.descriptor.manifest_id,
                0,
            )
            .await
            .unwrap();
    assert!(imports.iter().all(|proof| proof.delivery_id != "p73-fresh"));
    assert!(
        !prepared.messages.iter().any(|message| message
            .provenance
            .as_ref()
            .is_some_and(|p| p.sources.iter().any(|source| source.id == f.checkpoint))),
        "old proofs cannot substitute a missing current acceptance"
    );
}
#[tokio::test]
async fn obsolete_retained_raw_source_blocks_fresh_delivery_checkpoint_import() {
    let f = fixture().await;
    materialize(
        &f.h,
        "p73-recipient",
        "p73-recipient-turn",
        "recipient",
        true,
    )
    .await;
    delivery(&f.h, "p73-fresh", "p73-recipient", "p73-recipient-turn").await;
    expire_and_sweep(&f).await;
    f.h.crud_store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: f.h.workspace_id.clone(),
                thread_id: "p73-source".into(),
                turn_id: "p73-source-turn".into(),
                item: TurnItem::AgentMessage {
                    id: "p73-source-turn-answer".into(),
                    text: "changed after retained output".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            phase_13_now_secs() + 5,
        )
        .await
        .unwrap();
    assert!(
        capture_current(
            &f,
            "p73-recipient",
            "p73-recipient-turn",
            &authenticated_test_superuser()
        )
        .await
        .is_err(),
        "fresh transfer still requires current exact output sources"
    );
    // The owner's historical summary does not require the covered raw revision
    // to remain current; this is a separate production-entry contract.
    let own = capture_current(
        &f,
        "p73-source",
        "p73-source-turn",
        &authenticated_test_superuser(),
    )
    .await
    .unwrap();
    assert!(
        own.messages
            .iter()
            .any(|message| message.content.contains("P73 PERMANENT SUMMARY"))
    );
}

#[tokio::test]
async fn larger_checkpoint_closure_does_not_expand_one_retained_output_grant() {
    use pioneer_crud::compaction::{CanonicalSource, CommitOutcome, SourceAssertion};
    let f = fixture().await;
    materialize(
        &f.h,
        "p73-source",
        "p73-extra",
        "outside retained grant",
        true,
    )
    .await;
    let event =
        f.h.crud_store
            .compaction_source_page(
                &f.h.workspace_id,
                "p73-source",
                "p73-extra",
                PagedSource::Event,
                0,
            )
            .await
            .unwrap()
            .entries
            .into_iter()
            .find(|row| row.item_id.as_deref() == Some("p73-extra-answer"))
            .unwrap();
    let assertion = SourceAssertion {
        revision: Some(
            event
                .reference
                .version
                .strip_prefix("event-revision:")
                .unwrap()
                .parse()
                .unwrap(),
        ),
        kind: CanonicalSource::Event,
        turn_id: "p73-extra".into(),
        id: event.reference.id.clone(),
        payload: event.payload.unwrap(),
    };
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "summary-capture".into(),
        model: "test-model".into(),
        effort: None,
    };
    let operation = OperationSnapshot {
        id: "p73-larger-op".into(),
        owner: crate::compaction::native_owner(&f.h.workspace_id, "p73-source"),
        expected_checkpoint: Some(f.checkpoint.clone()),
        projection_version: f
            .h
            .crud_store
            .compaction_projection_version(&f.h.workspace_id, "p73-source")
            .await
            .unwrap(),
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(
                &selection,
                None,
                chrono::Utc::now().timestamp_millis() as u64,
            )
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::WorkingContext,
            compact: vec![0],
            retain: vec![],
            coverage: vec![assertion.reference()],
            fingerprint: "larger closure".into(),
        },
    };
    f.h.crud_store
        .compaction_admit(&f.h.workspace_id, "p73-source", &operation)
        .await
        .unwrap();
    let larger = pioneer_compaction::Checkpoint {
        id: "p73-larger".into(),
        operation_id: operation.id.clone(),
        owner: operation.owner.clone(),
        previous: Some(f.checkpoint.clone()),
        coverage: vec![assertion.reference()],
        summary: "summary contains additional work outside retained output".into(),
        selection,
        projection_version: operation.projection_version,
        format_version: 1,
    };
    f.h.crud_store
        .compaction_save_candidate(&larger, 0)
        .await
        .unwrap();
    assert_eq!(
        f.h.crud_store
            .compaction_apply(&larger, Some(&f.checkpoint), &[assertion])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    materialize(
        &f.h,
        "p73-recipient",
        "p73-recipient-turn",
        "recipient",
        true,
    )
    .await;
    delivery(&f.h, "p73-fresh", "p73-recipient", "p73-recipient-turn").await;
    expire_and_sweep(&f).await;
    let prepared = capture_current(
        &f,
        "p73-recipient",
        "p73-recipient-turn",
        &authenticated_test_superuser(),
    )
    .await
    .unwrap();
    let refs =
        f.h.crud_store
            .compaction_frozen_history_page(
                &f.h.workspace_id,
                "p73-recipient",
                &prepared.descriptor.manifest_id,
                0,
            )
            .await
            .unwrap();
    assert!(
        !refs.iter().any(|r| r
            .sources
            .iter()
            .any(|s| s.id == larger.id && s.scope.starts_with("checkpoint:"))),
        "fresh branch may choose a narrower ancestor or raw output, but not the larger closure"
    );
    assert!(
        !prepared
            .messages
            .iter()
            .any(|m| m.content.contains("outside retained output"))
    );
}

async fn capture_current(
    f: &FrozenFixture,
    thread: &str,
    turn: &str,
    principal: &crate::auth::AuthenticatedSessionPrincipal,
) -> anyhow::Result<crate::compaction::frozen::PreparedHistory> {
    persist_test_execution_authorization_context_for_principal(
        &f.h.processor,
        principal,
        &f.h.workspace_id,
        thread,
        turn,
    )
    .await;
    f.h.processor
        .capture_current_context_basis_prepared(
            &f.h.crud_store,
            &f.h.workspace_id,
            thread,
            turn,
            None,
        )
        .await
}
async fn commit_current(
    f: &FrozenFixture,
    thread: &str,
    turn: &str,
    prepared: &crate::compaction::frozen::PreparedHistory,
) {
    let now = chrono::Utc::now().fixed_offset();
    f.h.crud_store
        .upsert_turn_runtime_snapshot(pioneer_crud::NewTurnRuntimeSnapshot {
            turn_id: turn.into(),
            thread_id: thread.into(),
            workspace_id: f.h.workspace_id.clone(),
            mode_json: "\"Agent\"".into(),
            model: "fixture".into(),
            provider_name: "fixture".into(),
            reasoning_effort: None,
            agent_skill_versions_json: None,
            hook_runtime_context_json: "{}".into(),
            workspace_skill_policies_json: "[]".into(),
            input_json: "[]".into(),
            capabilities_json: "[]".into(),
            resolved_artifacts_json: "[]".into(),
            runtime_environment_json: "{}".into(),
            history_json: serde_json::to_string(&prepared.descriptor).unwrap(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let row =
        f.h.crud_store
            .get_turn_runtime_snapshot(turn)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        row.history_json,
        serde_json::to_string(&prepared.descriptor).unwrap()
    );
}

#[tokio::test]
async fn aggregate_large_checkpoint_keeps_nonempty_imports_through_legacy_backfill_publication_and_expired_reader()
 {
    for legacy in [false, true] {
        let f = fixture_with_correction(CorrectionFixture::Aggregate { legacy }).await;
        let db = f.h.crud_store.database_connection();
        for sql in [
            "SELECT sum(length(CAST(source_scope AS BLOB))+length(CAST(source_id AS BLOB))+length(CAST(source_version AS BLOB))) AS bytes FROM compaction_coverage WHERE checkpoint_id=?1",
            "SELECT sum(length(CAST(m.source_scope AS BLOB))+length(CAST(m.source_id AS BLOB))+length(CAST(m.source_version AS BLOB))+length(CAST(m.source_thread AS BLOB))) AS bytes FROM compaction_manifest m JOIN compaction_checkpoint p ON p.operation_id=m.operation_id WHERE p.id=?1 AND m.reference_only=0",
        ] {
            let bytes: i64 = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    sql,
                    [f.checkpoint.clone().into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "bytes")
                .unwrap();
            assert!(bytes > pioneer_crud::compaction::SOURCE_PAGE_BYTES as i64);
        }
        let before =
            f.h.crud_store
                .compaction_checkpoint_edges(&f.checkpoint)
                .await
                .unwrap()
                .unwrap();
        expire_and_sweep(&f).await;
        let after =
            f.h.crud_store
                .compaction_checkpoint_edges(&f.checkpoint)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(before.coverage, after.coverage);
        assert_eq!(before.event_input_evidence, after.event_input_evidence);
        materialize(&f.h, "p73-source", "p73-large-tail", "P73 LARGE TAIL", true).await;
        let prepared = capture_current(
            &f,
            "p73-source",
            "p73-large-tail",
            &authenticated_test_superuser(),
        )
        .await
        .unwrap();
        commit_current(&f, "p73-source", "p73-large-tail", &prepared).await;
        assert!(
            prepared
                .messages
                .iter()
                .any(|m| m.content.contains("P73 PERMANENT SUMMARY"))
        );
        assert_ne!(prepared.descriptor.manifest_id, f.origin.manifest_id);
    }
}
#[tokio::test]
async fn selected_import_understated_retained_output_direct_and_shared_fails_before_payload_seal_and_publication()
 {
    for shared in [false, true] {
        let _ = fixture_with_correction(CorrectionFixture::UnderstatedOutput { shared }).await;
    }
}

#[tokio::test(start_paused = true)]
async fn automatic_frozen_worker_restart_prepares_legacy_proofs_expires_and_sweeps_with_fk_off() {
    let f = fixture_with_correction(CorrectionFixture::WorkerLegacy).await;
    let db = f.h.crud_store.database_connection();
    // Run the actual production loop, cancel it, and restart with fresh private
    // progress. No fixture calls quantum: durable markers must survive restart.
    for restart in 0..2 {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let worker = tokio::spawn(crate::database::maintenance::run_frozen_worker_for_test(
            f.h.crud_store.clone(),
            cancellation.clone(),
        ));
        let mut swept = false;
        let mut interrupted_staging = false;
        for _ in 0..20_000 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
            let row = db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "SELECT h.expired,c.proof_version,(SELECT count(*) FROM compaction_frozen_message_data WHERE manifest_id=h.id)+(SELECT count(*) FROM compaction_frozen_import_data WHERE manifest_id=h.id) AS n FROM compaction_frozen_history h JOIN compaction_checkpoint c ON c.id=?2 WHERE h.id=?1",
                [f.origin.manifest_id.clone().into(), f.checkpoint.clone().into()])).await.unwrap().unwrap();
            swept = row.try_get::<i64>("", "expired").unwrap() == 1
                && row.try_get::<i64>("", "proof_version").unwrap() == 1
                && row.try_get::<i64>("", "n").unwrap() == 0;
            let selected: i64 = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT count(*) AS n FROM compaction_checkpoint_import WHERE checkpoint_id=?1",
                    [f.checkpoint.clone().into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "n")
                .unwrap();
            interrupted_staging =
                selected > 0 && row.try_get::<i64>("", "proof_version").unwrap() == 0;
            if (restart == 0 && interrupted_staging) || (restart == 1 && swept) {
                break;
            }
        }
        cancellation.cancel();
        worker.await.unwrap();
        if restart == 0 {
            assert!(
                interrupted_staging,
                "restart interrupts real nonempty unsealed preparation"
            );
        }
        if restart == 1 {
            assert!(
                swept,
                "automatic worker must finish legacy preparation and physical deletion"
            );
        }
    }
    let n: i64 = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint_import WHERE checkpoint_id=?1",
            [f.checkpoint.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert!(n > 0, "nonempty permanent evidence survives sweep");
    assert!(
        !f.h.crud_store
            .compaction_frozen_history_page(
                &f.h.workspace_id,
                "p73-source",
                &f.output.history.manifest_id,
                0
            )
            .await
            .unwrap()
            .is_empty(),
        "retained output remains readable"
    );
}

#[tokio::test(start_paused = true)]
async fn idle_frozen_worker_cancellation_interrupts_backoff_without_detached_work() {
    let (_directory, _, store, _) = setup_pooled_file_workspace_manager().await;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(crate::database::maintenance::run_frozen_worker_for_test(
        store,
        cancellation.clone(),
    ));
    // Empty discovery reaches the idle round; cancellation must wake the actual
    // production sleep select rather than waiting for its 60 second timer.
    for _ in 0..100 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert!(!worker.is_finished());
    cancellation.cancel();
    tokio::time::timeout(Duration::from_millis(1), worker)
        .await
        .unwrap()
        .unwrap();
}
