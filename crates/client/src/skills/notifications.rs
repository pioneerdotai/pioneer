//! Skill notification refresh decisions.

pub use crate::notifications::router::{
    SkillsRefreshReduction, reduce_skills_changed_notification,
};

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::SkillsChangedNotification;
    use serde_json::json;

    #[test]
    fn old_skills_changed_payload_defaults_pack_changes_and_still_invalidates() {
        let notification: SkillsChangedNotification = serde_json::from_value(json!({
            "workspace_id": "workspace",
            "snapshot_version": 2,
            "reason": "skill_updated",
            "changes": [],
            "created_at": 3
        }))
        .expect("old skills/changed payload");

        assert!(notification.pack_changes.is_empty());
        let reduction = reduce_skills_changed_notification(notification, Some("workspace"));
        assert!(reduction.queue_skills_refresh);
        assert_eq!(reduction.workspace_id, "workspace");
    }
}
