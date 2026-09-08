use super::TimelineRenderModel;
use crate::screen::TimelineView;

pub(crate) use pioneer_client::timeline::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX;

impl TimelineView {
    pub(crate) fn semantic_timeline_render_model(
        &self,
        active_thread_id: Option<&str>,
    ) -> TimelineRenderModel {
        if active_thread_id == Some(self.thread_id.as_str()) {
            self.thread_timeline_view_state.model.clone()
        } else {
            TimelineRenderModel::empty()
        }
    }
}

impl TimelineRenderModel {
    pub(crate) fn from_snapshot(
        snapshot: &pioneer_client::timeline::presentation::TimelineSnapshot,
    ) -> Self {
        Self {
            revision: snapshot.revision(),
            source_revision: snapshot.source_revision(),
            item_presentations: std::sync::Arc::new(
                snapshot
                    .rows()
                    .iter()
                    .filter_map(|row| Some((row.item()?.id.clone(), row.content()?.clone())))
                    .collect(),
            ),
            groups: snapshot.groups(),
            projection: snapshot.projection(),
            rows: snapshot.render_rows(),
            row_revisions: snapshot.row_revisions(),
        }
    }
}
