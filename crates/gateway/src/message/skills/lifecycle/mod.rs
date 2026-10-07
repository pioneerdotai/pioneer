use super::*;

mod install;
mod pack;
mod pack_install;
mod pack_uninstall;
mod pack_update;
mod uninstall;
mod update;

pub(crate) mod source;

// Committed changes are published by the operation's caller. Standalone RPCs
// acknowledge first; plugin operations publish before returning to their parent.
struct SkillChangePublication {
    workspace_id: String,
    reason: &'static str,
    changes: Vec<SkillChangedItem>,
    pack_changes: Vec<SkillPackChangedItem>,
    created_at: i64,
}
impl MessageProcessor {
    async fn publish_skill_change(&self, publication: SkillChangePublication) {
        self.notify_skill_projection_changed(
            &publication.workspace_id,
            publication.reason,
            publication.changes,
            publication.pack_changes,
            publication.created_at,
        )
        .await;
    }
}
