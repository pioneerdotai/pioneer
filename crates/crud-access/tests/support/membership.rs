use pioneer_entity::{gateway_principal, thread, workspace};
use pioneer_protocol::{
    AUTH_DOMAIN_ID_LEN, GATEWAY_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PrincipalId, generate_id,
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};

pub struct MembershipFixture {
    pub database: DatabaseConnection,
    pub gateway_id: GatewayId,
    pub superuser_id: PrincipalId,
    pub member_id: PrincipalId,
    pub red_workspace_id: String,
    pub blue_workspace_id: String,
    pub private_thread_id: String,
}

pub async fn membership_fixture(connection: &DatabaseConnection) -> MembershipFixture {
    let database = connection.clone();
    workspace::Entity::delete_by_id(super::domain::workspace::DEFAULT_WORKSPACE_ID)
        .exec(&database)
        .await
        .expect("remove default workspace from membership fixture");
    let now = chrono::Utc::now().fixed_offset();
    let gateway_id = GatewayId::new(generate_id(GATEWAY_ID_LEN)).expect("gateway id");
    let superuser_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("superuser id");
    let member_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("member id");
    super::domain::identity::create_gateway_singleton(&database, &gateway_id, 1, now)
        .await
        .expect("create gateway");
    super::domain::identity::create_superuser(
        &database,
        &superuser_id,
        &gateway_id,
        "Superuser",
        "superuser",
        "superuser",
        now,
        0,
    )
    .await
    .expect("create superuser");
    gateway_principal::ActiveModel {
        id: Set(member_id.to_string()),
        gateway_id: Set(gateway_id.to_string()),
        kind: Set("user".to_owned()),
        role_key: Set(Some("member".to_owned())),
        status: Set("active".to_owned()),
        display_name: Set("Member".to_owned()),
        nickname: Set("member".to_owned()),
        nickname_key: Set("member".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
        removed_at: Set(None),
        authorization_guard: Set(1),
        ..Default::default()
    }
    .insert(&database)
    .await
    .expect("create test-only Member");

    let red_workspace_id = generate_id(AUTH_DOMAIN_ID_LEN);
    let blue_workspace_id = generate_id(AUTH_DOMAIN_ID_LEN);
    for (id, name) in [
        (red_workspace_id.as_str(), "Red"),
        (blue_workspace_id.as_str(), "Blue"),
    ] {
        workspace::ActiveModel {
            id: Set(id.to_owned()),
            name: Set(name.to_owned()),
            is_active: Set(true),
            is_current: Set(false),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&database)
        .await
        .expect("create workspace");
    }

    let private_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
    thread::ActiveModel {
        workspace_unread_started_at: sea_orm::NotSet,
        id: Set(private_thread_id.clone()),
        workspace_id: Set(red_workspace_id.clone()),
        name: Set(Some("Private".to_owned())),
        preview: Set(String::new()),
        preview_author_json: Set(None),
        mode: Set("chat".to_owned()),
        model: Set("test".to_owned()),
        model_provider: Set("test".to_owned()),
        status: Set("active".to_owned()),
        origin_kind: Set("user".to_owned()),
        sidebar_visibility: Set("visible".to_owned()),
        access_class: Set("private".to_owned()),
        agent_nickname: Set(None),
        agent_role: Set(None),
        summary: Set(None),
        summary_turn_count: Set(Some(0)),
        created_at: Set(now),
        updated_at: Set(now),
        created_by_actor_id: Set(Some(superuser_id.to_string())),
        created_by_actor_kind: Set(Some("principal".to_owned())),
        ..Default::default()
    }
    .insert(&database)
    .await
    .expect("create private thread");

    MembershipFixture {
        database,
        gateway_id,
        superuser_id,
        member_id,
        red_workspace_id,
        blue_workspace_id,
        private_thread_id,
    }
}
