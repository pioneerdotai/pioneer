use super::TimelineRenderModel;

pub(crate) use pioneer_client::timeline::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX;

impl TimelineRenderModel {
    pub(crate) fn from_snapshot(
        snapshot: &pioneer_client::timeline::presentation::TimelineSnapshot,
    ) -> Self {
        Self {
            snapshot: Some(std::sync::Arc::new(snapshot.clone())),
            revision: snapshot.revision(),
            source_revision: snapshot.source_revision(),
            item_presentations: std::sync::Arc::new(
                snapshot
                    .rows()
                    .iter()
                    .filter_map(|row| Some((row.item()?.id.clone(), row.clone())))
                    .collect(),
            ),
            groups: snapshot.groups(),
            projection: snapshot.projection(),
            rows: snapshot.render_rows(),
        }
    }
}
