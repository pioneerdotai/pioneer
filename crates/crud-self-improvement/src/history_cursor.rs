use anyhow::Result;
use pioneer_entity::self_improvement_workspace_state as state;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, QuerySelect, Set};

use crate::self_improvement_run;

/// Discovery can be stale, but rewinding farther is harmless. Revalidate cursor/epoch and the
/// absence of an unresolved frozen run so a concurrent finalization or retry is never reset.
pub async fn rewind_idle<C: ConnectionTrait>(
    db: &C,
    expected: &state::Model,
    earliest: i64,
) -> Result<()> {
    let Some(current) = state::Entity::find_by_id(expected.workspace_id.clone())
        .lock_exclusive()
        .one(db)
        .await?
    else {
        return Ok(());
    };
    if current.effective_enabled_at.is_none()
        || current.activation_epoch != expected.activation_epoch
        || current.cursor_source_id != expected.cursor_source_id
        || earliest > current.cursor_source_id
        || self_improvement_run::find_oldest_unresolved(
            db,
            &current.workspace_id,
            current.activation_epoch,
        )
        .await?
        .is_some()
    {
        return Ok(());
    }
    let mut update: state::ActiveModel = current.into();
    update.cursor_source_id = Set(earliest - 1);
    update.update(db).await?;
    Ok(())
}
