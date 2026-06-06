use super::content::{
    agents_doc_content_hash, agents_doc_initial_buffer, agents_doc_is_version_conflict_error,
};
use pioneer_client::agents_doc::autosave::{
    AgentsDocAutosaveDecision, AgentsDocAutosaveState, AgentsDocEditorSaveState,
};
use pioneer_protocol::{ThreadAgentsDocPayload, ThreadAgentsDocStatus};

fn payload(status: ThreadAgentsDocStatus, content: &str) -> ThreadAgentsDocPayload {
    ThreadAgentsDocPayload {
        id: "agd_1".to_owned(),
        workspace_id: "ws_1".to_owned(),
        folder_id: Some("fld_1".to_owned()),
        status,
        title: "AGENTS.md".to_owned(),
        content: content.to_owned(),
        content_sha256: agents_doc_content_hash(content),
        version: 1,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
    }
}

#[::core::prelude::v1::test]
fn agents_doc_editor_initial_buffer_uses_explicit_content() {
    let explicit = payload(ThreadAgentsDocStatus::Active, "Use pnpm.");
    assert_eq!(
        agents_doc_initial_buffer(Some(&explicit)),
        "Use pnpm.".to_owned()
    );
}

#[::core::prelude::v1::test]
fn agents_doc_editor_initial_buffer_is_empty_for_inherited_only() {
    assert_eq!(agents_doc_initial_buffer(None), String::new());
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_change_marks_dirty_and_schedules() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));

    let decision = state.mark_changed("new");

    assert_eq!(
        decision,
        AgentsDocAutosaveDecision::Schedule { generation: 2 }
    );
    assert_eq!(state.save_state, AgentsDocEditorSaveState::Dirty);
    assert_eq!(state.pending_hash, Some(agents_doc_content_hash("new")));
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_same_hash_skips_save() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(
        ThreadAgentsDocStatus::Active,
        "line\nending",
    )));

    let decision = state.mark_changed("line\r\nending");

    assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
    assert_eq!(state.save_state, AgentsDocEditorSaveState::Clean);
    assert_eq!(state.pending_hash, None);
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_success_updates_version_and_hash() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
    assert!(matches!(
        state.flush("new"),
        AgentsDocAutosaveDecision::SaveNow { .. }
    ));
    state.mark_saving();
    let mut saved = payload(ThreadAgentsDocStatus::Active, "new");
    saved.version = 2;

    let decision = state.finish_success(&saved, agents_doc_content_hash("new").as_str(), 42);

    assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
    assert_eq!(state.last_saved_hash, Some(agents_doc_content_hash("new")));
    assert_eq!(state.last_saved_version, Some(2));
    assert_eq!(
        state.save_state,
        AgentsDocEditorSaveState::Saved { saved_at: 42 }
    );
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_failed_save_keeps_dirty_hash_and_error() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
    assert!(matches!(
        state.flush("new"),
        AgentsDocAutosaveDecision::SaveNow { .. }
    ));
    state.mark_saving();

    state.finish_error("offline".to_owned());

    assert_eq!(state.pending_hash, Some(agents_doc_content_hash("new")));
    assert_eq!(state.last_saved_version, Some(1));
    assert_eq!(
        state.save_state,
        AgentsDocEditorSaveState::Error {
            message: "offline".to_owned()
        }
    );
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_stale_debounce_generation_noops() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
    let first = match state.mark_changed("new 1") {
        AgentsDocAutosaveDecision::Schedule { generation } => generation,
        other => panic!("expected schedule, got {other:?}"),
    };
    let second = match state.mark_changed("new 2") {
        AgentsDocAutosaveDecision::Schedule { generation } => generation,
        other => panic!("expected schedule, got {other:?}"),
    };

    assert!(!state.debounce_due(first));
    assert!(state.debounce_due(second));
}

#[::core::prelude::v1::test]
fn agents_doc_autosave_flush_runs_pending_save_immediately() {
    let mut state = AgentsDocAutosaveState::new();
    state.reset_from_explicit(Some(&payload(ThreadAgentsDocStatus::Active, "old")));
    assert!(matches!(
        state.mark_changed("new"),
        AgentsDocAutosaveDecision::Schedule { .. }
    ));

    let decision = state.flush("new");

    assert_eq!(
        decision,
        AgentsDocAutosaveDecision::SaveNow { generation: 3 }
    );
}

#[::core::prelude::v1::test]
fn agents_doc_conflict_stops_autosave() {
    let mut state = AgentsDocAutosaveState::new();
    let remote = payload(ThreadAgentsDocStatus::Active, "remote");
    state.enter_conflict("local".to_owned(), remote);

    let decision = state.mark_changed("local edited");

    assert_eq!(decision, AgentsDocAutosaveDecision::Noop);
    assert!(!state.debounce_due(state.generation));
    assert!(matches!(
        state.save_state,
        AgentsDocEditorSaveState::Conflict { .. }
    ));
}

#[::core::prelude::v1::test]
fn agents_doc_conflict_reload_remote_replaces_local_state() {
    let mut state = AgentsDocAutosaveState::new();
    let remote = payload(ThreadAgentsDocStatus::Active, "remote");
    state.enter_conflict("local".to_owned(), remote.clone());

    state.reload_remote(&remote);

    assert_eq!(state.save_state, AgentsDocEditorSaveState::Clean);
    assert_eq!(state.last_saved_hash, Some(remote.content_sha256.clone()));
    assert_eq!(state.last_saved_version, Some(remote.version));
    assert_eq!(
        agents_doc_initial_buffer(Some(&remote)),
        "remote".to_owned()
    );
}

#[::core::prelude::v1::test]
fn agents_doc_conflict_overwrite_remote_resumes_saved_state() {
    let mut state = AgentsDocAutosaveState::new();
    let remote = payload(ThreadAgentsDocStatus::Active, "remote");
    state.enter_conflict("local".to_owned(), remote);

    let decision = state.prepare_conflict_overwrite("local");
    state.mark_saving();
    let mut saved = payload(ThreadAgentsDocStatus::Active, "local");
    saved.version = 3;
    let finish = state.finish_success(&saved, agents_doc_content_hash("local").as_str(), 99);

    assert!(matches!(
        decision,
        AgentsDocAutosaveDecision::SaveNow { .. }
    ));
    assert_eq!(finish, AgentsDocAutosaveDecision::Noop);
    assert_eq!(
        state.save_state,
        AgentsDocEditorSaveState::Saved { saved_at: 99 }
    );
    assert_eq!(state.last_saved_version, Some(3));
}

#[::core::prelude::v1::test]
fn agents_doc_conflict_detects_version_conflict_error() {
    let error = anyhow::anyhow!(
        "failed to process `thread/agents_doc/save`: version conflict, expected 1, actual 2"
    );

    assert!(agents_doc_is_version_conflict_error(&error));
}
