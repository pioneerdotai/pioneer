use std::sync::Arc;

use axum::extract::ws::Message;
use pioneer_crud::CrudStore;
use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind};
use sea_orm::ConnectionTrait;
use tokio::sync::mpsc;

use super::{ConnectionId, SessionManager};
use crate::auth::AuthenticatedSessionPrincipal;

pub(crate) const TEST_GATEWAY_ID: &str = "G00000000000000000001";
pub(crate) const TEST_SUPERUSER_PRINCIPAL_ID: &str = "P00000000000000000001";

pub(crate) fn authenticated_test_superuser() -> Arc<AuthenticatedSessionPrincipal> {
    Arc::new(AuthenticatedSessionPrincipal {
        gateway_id: GatewayId::new(TEST_GATEWAY_ID).expect("valid deterministic Gateway id"),
        principal_id: PrincipalId::new(TEST_SUPERUSER_PRINCIPAL_ID)
            .expect("valid deterministic Principal id"),
        kind: PrincipalKind::Superuser,
        role_key: None,
        device_id: DeviceId::new("D00000000000000000001").unwrap(),
        session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
        access_jti: "J00000000000000000001".to_owned(),
        access_expires_at_unix: u64::MAX,
    })
}

pub(crate) fn authenticated_test_superuser_secondary_session() -> Arc<AuthenticatedSessionPrincipal>
{
    Arc::new(AuthenticatedSessionPrincipal {
        gateway_id: GatewayId::new(TEST_GATEWAY_ID).expect("valid deterministic Gateway id"),
        principal_id: PrincipalId::new(TEST_SUPERUSER_PRINCIPAL_ID)
            .expect("valid deterministic Principal id"),
        kind: PrincipalKind::Superuser,
        role_key: None,
        device_id: DeviceId::new("D00000000000000000002").unwrap(),
        session_id: AuthSessionId::new("S00000000000000000002").unwrap(),
        access_jti: "J00000000000000000002".to_owned(),
        access_expires_at_unix: u64::MAX,
    })
}

pub(crate) async fn ensure_test_superuser_execution_authority(crud_store: &CrudStore) {
    crud_store
        .database_connection()
        .execute_unprepared(
            "INSERT OR IGNORE INTO gateway_identity(\
                id,singleton_key,identity_bootstrap_version,auth_schema_version,auth_ready_at,\
                created_at,updated_at\
             ) VALUES(\
                'G00000000000000000001',1,1,2,CURRENT_TIMESTAMP,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
             );\
             INSERT OR IGNORE INTO gateway_principal(\
                id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                created_at,updated_at,removed_at\
             ) VALUES(\
                'P00000000000000000001','G00000000000000000001','superuser',NULL,'active',\
                'Superuser','superuser','superuser',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
             );\
             INSERT OR IGNORE INTO device(\
                id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
             ) VALUES(\
                'D00000000000000000001','G00000000000000000001',\
                'P00000000000000000001','gateway-test-superuser','Gateway Test Superuser',\
                'desktop','test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                CURRENT_TIMESTAMP,NULL\
             );\
             INSERT OR IGNORE INTO auth_session(\
                id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                activation_expires_at,activated_at,status,refresh_generation,created_at,\
                updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                revoke_reason\
             ) VALUES(\
                'S00000000000000000001','G00000000000000000001',\
                'P00000000000000000001','D00000000000000000001',\
                'F00000000000000000001',NULL,randomblob(32),randomblob(32),0,\
                datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                datetime('now','+90 days'),NULL,NULL\
             );\
             INSERT OR IGNORE INTO auth_refresh_credential(\
                id,session_id,token_family_id,generation,token_hash,issued_at,expires_at\
             ) VALUES(\
                'R00000000000000000001','S00000000000000000001',\
                'F00000000000000000001',0,randomblob(32),CURRENT_TIMESTAMP,\
                datetime('now','+90 days')\
             );",
        )
        .await
        .expect("test Superuser execution authority should materialize");
}

pub(crate) async fn ensure_test_superuser_secondary_session_authority(crud_store: &CrudStore) {
    ensure_test_superuser_execution_authority(crud_store).await;
    crud_store
        .database_connection()
        .execute_unprepared(
            "INSERT OR IGNORE INTO device(\
                id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
             ) VALUES(\
                'D00000000000000000002','G00000000000000000001',\
                'P00000000000000000001','gateway-test-superuser-secondary',\
                'Gateway Test Superuser Secondary','mobile','test','1','active',\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
             );\
             INSERT OR IGNORE INTO auth_session(\
                id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                activation_expires_at,activated_at,status,refresh_generation,created_at,\
                updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                revoke_reason\
             ) VALUES(\
                'S00000000000000000002','G00000000000000000001',\
                'P00000000000000000001','D00000000000000000002',\
                'F00000000000000000002','S00000000000000000001',randomblob(32),\
                randomblob(32),0,datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                datetime('now','+90 days'),NULL,NULL\
             );\
             INSERT OR IGNORE INTO auth_refresh_credential(\
                id,session_id,token_family_id,generation,token_hash,issued_at,expires_at\
             ) VALUES(\
                'R00000000000000000002','S00000000000000000002',\
                'F00000000000000000002',0,randomblob(32),CURRENT_TIMESTAMP,\
                datetime('now','+90 days')\
             );",
        )
        .await
        .expect("secondary test Superuser session authority should materialize");
}

pub(crate) async fn register_authenticated_test_connection(
    manager: &SessionManager,
    sender: mpsc::Sender<Message>,
) -> ConnectionId {
    manager
        .register_connection(sender, authenticated_test_superuser())
        .await
        .expect("test auth session must be registerable")
}
