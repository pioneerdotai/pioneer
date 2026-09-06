use super::TimelineRenderModel;
use crate::app::root::PioneerDesktop;

pub(in crate::app::thread::view::timeline) use pioneer_client::timeline::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX;

impl PioneerDesktop {
    pub(crate) fn semantic_timeline_render_model(
        &self,
        active_thread_id: Option<&str>,
    ) -> TimelineRenderModel {
        self.thread_bindings
            .timeline_model(active_thread_id)
            .unwrap_or_else(TimelineRenderModel::empty)
    }
}

impl TimelineRenderModel {
    pub(in crate::app) fn from_snapshot(
        snapshot: &pioneer_client::timeline::presentation::TimelineSnapshot,
    ) -> Self {
        Self {
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
