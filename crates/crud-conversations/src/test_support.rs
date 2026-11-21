use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, sea_query::Expr};

use crate::thread_timeline_projection::{
    BLOCK_KIND_USER_MESSAGE, ThreadTimelineBlockRecord, upsert_thread_timeline_block,
};
use crate::values::unix_to_datetime;

pub async fn insert_unread_test_turn(
    db: &impl ConnectionTrait,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    sort_key: &str,
    send_mode: &str,
    actor_id: Option<&str>,
    deleted: bool,
) {
    // This fixture's synthetic event times predate wall-clock migration.
    // Set its rollout explicitly; never manufacture a read receipt.
    pioneer_entity::gateway_identity::Entity::update_many()
        .col_expr(
            pioneer_entity::gateway_identity::Column::UnreadStartedAt,
            Expr::val(0),
        )
        .filter(pioneer_entity::gateway_identity::Column::SingletonKey.eq(1))
        .exec(db)
        .await
        .unwrap();
    let created_at = unix_to_datetime(1_700_000_100);
    pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
        id: Set(turn_id.to_owned()),
        thread_id: Set(thread_id.to_owned()),
        package_selection_json: Set(None),
        status: Set("completed".to_owned()),
        error: Set(None),
        prompt_manifest_json: Set("{}".to_owned()),
        prompt_compiler_version: Set(None),
        prompt_profile: Set(None),
        prompt_fingerprint_stable: Set(None),
        prompt_fingerprint_dynamic: Set(None),
        prompt_fingerprint_full: Set(None),
        created_at: Set(created_at),
        updated_at: Set(created_at),
        turn_kind: Set("conversation".to_owned()),
        origin: Set("user".to_owned()),
        work_owner: Set("turn".to_owned()),
        reasoning_effort: Set(None),
        permission_profile_mode: Set(None),
        permission_profile_source: Set(None),
        permission_profile_snapshot_json: Set(None),
        execution_security_snapshot_version: Set(None),
        execution_security_snapshot_json: Set(None),
        initiated_by_actor_id: Set(actor_id.map(str::to_owned)),
        initiated_by_actor_kind: Set(Some(
            if actor_id.is_some() {
                "principal"
            } else {
                "system"
            }
            .to_owned(),
        )),
        execution_authorization_context_json: Set(None),
        send_mode: Set(Some(send_mode.to_owned())),
        author_display_name_snapshot: Set(Some("Author".to_owned())),
        author_nickname_snapshot: Set(Some("author".to_owned())),
        author_avatar_revision_snapshot: Set(None),
        author_agent_snapshot_json: Set(None),
        reply_to_turn_id: Set(None),
        mentions_json: Set("[]".to_owned()),
        message_revision: Set(0),
        message_deleted_at: Set(deleted.then_some(created_at)),
        message_deleted_by_actor_id: Set(None),
        message_deleted_by_actor_kind: Set(None),
        ..Default::default()
    })
    .exec(db)
    .await
    .expect("unread test Turn should persist");
    pioneer_entity::unread_message_position::Entity::insert(
        pioneer_entity::unread_message_position::ActiveModel {
            thread_id: Set(thread_id.into()),
            block_id: Set(format!("user:{turn_id}")),
            turn_id: Set(turn_id.into()),
            sort_key: Set(sort_key.into()),
            published_at: Set(created_at.timestamp()),
        },
    )
    .on_conflict(
        sea_orm::sea_query::OnConflict::columns([
            pioneer_entity::unread_message_position::Column::ThreadId,
            pioneer_entity::unread_message_position::Column::BlockId,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .unwrap();
    upsert_thread_timeline_block(
        db,
        ThreadTimelineBlockRecord {
            block_id: format!("user:{turn_id}"),
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: Some(turn_id.to_owned()),
            block_kind: BLOCK_KIND_USER_MESSAGE.to_owned(),
            sort_key: sort_key.to_owned(),
            source_kind: Some("turn".to_owned()),
            source_key: Some(turn_id.to_owned()),
            started_at: Some(created_at),
            completed_at: Some(created_at),
            metadata_json: "{}".to_owned(),
            created_at,
            updated_at: created_at,
        },
    )
    .await
    .expect("unread test timeline block should persist");
}
