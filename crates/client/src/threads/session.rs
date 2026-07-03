//! Shared thread session lifecycle rules.
//!
//! This module is intentionally free of FFI, React Native and GPUI types.
//! Shells keep their own runtime state, but active/draft/remembered-thread
//! rules introduced here must be applied through these helpers.

use std::collections::HashMap;

use super::tree;

pub fn set_active_thread_id(
    active_thread_id: &mut Option<String>,
    thread_id: Option<String>,
) -> bool {
    let changed = *active_thread_id != thread_id;
    *active_thread_id = thread_id;
    changed
}

pub fn clear_active_thread_if_matches(
    active_thread_id: &mut Option<String>,
    thread_id: &str,
) -> bool {
    if active_thread_id.as_deref() != Some(thread_id) {
        return false;
    }

    *active_thread_id = None;
    true
}

pub fn set_draft_thread_id(draft_thread_id: &mut Option<String>, thread_id: Option<String>) {
    *draft_thread_id = thread_id;
}

pub fn clear_draft_thread_if_matches(
    draft_thread_id: &mut Option<String>,
    thread_id: &str,
) -> bool {
    if draft_thread_id.as_deref() != Some(thread_id) {
        return false;
    }

    *draft_thread_id = None;
    true
}

pub fn promote_thread_from_draft(draft_thread_id: &mut Option<String>, thread_id: &str) -> bool {
    clear_draft_thread_if_matches(draft_thread_id, thread_id)
}

pub fn resolve_existing_draft_thread_id(
    draft_thread_id: &mut Option<String>,
    has_thread: impl FnOnce(&str) -> bool,
) -> Option<String> {
    let thread_id = draft_thread_id.clone()?;
    if has_thread(thread_id.as_str()) {
        return Some(thread_id);
    }

    *draft_thread_id = None;
    None
}

pub fn remember_thread_for_workspace(
    remembered_threads: &mut HashMap<String, String>,
    workspace_id: &str,
    thread_id: Option<String>,
) -> bool {
    let previous =
        remembered_thread_for_workspace(remembered_threads, workspace_id).map(str::to_owned);
    if previous.as_deref() == thread_id.as_deref() {
        return false;
    }

    tree::remember_thread_for_workspace(remembered_threads, workspace_id, thread_id);
    true
}

pub fn remembered_thread_for_workspace<'a>(
    remembered_threads: &'a HashMap<String, String>,
    workspace_id: &str,
) -> Option<&'a str> {
    tree::remembered_thread_for_workspace(remembered_threads, workspace_id)
}

pub fn resolve_remembered_thread_for_workspace(
    remembered_threads: &mut HashMap<String, String>,
    workspace_id: &str,
    has_thread: impl FnOnce(&str) -> bool,
) -> Option<String> {
    let thread_id =
        remembered_thread_for_workspace(remembered_threads, workspace_id).map(str::to_owned)?;
    if has_thread(thread_id.as_str()) {
        return Some(thread_id);
    }

    remember_thread_for_workspace(remembered_threads, workspace_id, None);
    None
}

pub fn contains_thread_marker(
    remembered_threads: &HashMap<String, String>,
    thread_id: &str,
) -> bool {
    remembered_threads
        .values()
        .any(|remembered_thread_id| remembered_thread_id == thread_id)
}

pub fn clear_thread_markers(
    remembered_threads: &mut HashMap<String, String>,
    thread_id: &str,
) -> bool {
    let before = remembered_threads.len();
    remembered_threads.retain(|_, remembered_thread_id| remembered_thread_id != thread_id);
    remembered_threads.len() != before
}

pub fn require_thread_id(
    thread_id: Option<String>,
    action: &'static str,
) -> Result<String, String> {
    let Some(thread_id) = thread_id.and_then(|thread_id| {
        let trimmed = thread_id.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }) else {
        return Err(format!("thread_id is required before {action}"));
    };

    Ok(thread_id)
}

pub fn bump_session_revision(session_revision: &mut u64) {
    *session_revision = session_revision.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_thread_changes_only_when_identity_changes() {
        let mut active_thread_id = Some("thread_a".to_owned());

        assert!(!set_active_thread_id(
            &mut active_thread_id,
            Some("thread_a".to_owned())
        ));
        assert!(set_active_thread_id(
            &mut active_thread_id,
            Some("thread_b".to_owned())
        ));
        assert_eq!(active_thread_id.as_deref(), Some("thread_b"));
    }

    #[test]
    fn stale_draft_marker_is_cleared_when_thread_is_missing() {
        let mut draft_thread_id = Some("thread_a".to_owned());

        assert_eq!(
            resolve_existing_draft_thread_id(&mut draft_thread_id, |_| false),
            None
        );
        assert!(draft_thread_id.is_none());
    }

    #[test]
    fn workspace_thread_markers_are_keyed_by_workspace() {
        let mut remembered = HashMap::new();

        assert!(remember_thread_for_workspace(
            &mut remembered,
            "ws_a",
            Some("thread_a".to_owned())
        ));
        assert!(!remember_thread_for_workspace(
            &mut remembered,
            "ws_a",
            Some("thread_a".to_owned())
        ));
        assert!(remember_thread_for_workspace(
            &mut remembered,
            "ws_b",
            Some("thread_b".to_owned())
        ));
        assert!(clear_thread_markers(&mut remembered, "thread_a"));
        assert!(!clear_thread_markers(&mut remembered, "thread_a"));

        assert_eq!(remembered_thread_for_workspace(&remembered, "ws_a"), None);
        assert_eq!(
            remembered_thread_for_workspace(&remembered, "ws_b"),
            Some("thread_b")
        );
    }

    #[test]
    fn stale_workspace_thread_marker_is_cleared_when_thread_is_missing() {
        let mut remembered = HashMap::new();
        remember_thread_for_workspace(&mut remembered, "ws_a", Some("thread_a".to_owned()));

        assert_eq!(
            resolve_remembered_thread_for_workspace(&mut remembered, "ws_a", |_| false),
            None
        );
        assert_eq!(remembered_thread_for_workspace(&remembered, "ws_a"), None);

        remember_thread_for_workspace(&mut remembered, "ws_a", Some("thread_b".to_owned()));
        assert_eq!(
            resolve_remembered_thread_for_workspace(&mut remembered, "ws_a", |_| true),
            Some("thread_b".to_owned())
        );
        assert_eq!(
            remembered_thread_for_workspace(&remembered, "ws_a"),
            Some("thread_b")
        );
    }

    #[test]
    fn missing_thread_id_is_rejected_for_thread_bound_actions() {
        assert!(require_thread_id(None, "sending text").is_err());
        assert_eq!(
            require_thread_id(Some(" thread_a ".to_owned()), "sending text").unwrap(),
            "thread_a"
        );
    }
}
