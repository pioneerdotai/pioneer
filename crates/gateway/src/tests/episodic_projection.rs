//! Bounded observation of persisted derived text for queue/reset regressions.
use pioneer_crud::{CrudStore, ThreadEpisodicItemRecord};
use pioneer_entity::thread_episodic_items;
use sea_orm::EntityTrait;

pub(crate) trait PersistedProjection {
    async fn projection_text(&self, store: &CrudStore) -> Option<String>;
}

impl PersistedProjection for ThreadEpisodicItemRecord {
    async fn projection_text(&self, store: &CrudStore) -> Option<String> {
        let row = thread_episodic_items::Entity::find_by_id(&self.id)
            .one(&store.database_connection())
            .await
            .expect("read persisted projection")
            .expect("projection occurrence exists");
        assert_eq!(row.workspace_id, self.workspace_id);
        row.search_text
    }
}
