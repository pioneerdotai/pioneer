use std::collections::BTreeSet;

use anyhow::{Context, Result};
use pioneer_crud::CrudStore;
use pioneer_mcp::McpSecretRef;

use crate::secrets::{GatewaySecrets, McpSecretDeleteFailure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct McpSecretGarbageCollectionReport {
    pub oauth_stored: usize,
    pub oauth_orphans: usize,
    pub oauth_deleted: usize,
    pub active_refs: usize,
    pub stored_refs: usize,
    pub orphan_refs: usize,
    pub deleted_refs: usize,
    pub failed_deletes: Vec<McpSecretDeleteFailure>,
}

pub(crate) async fn garbage_collection_orphan_mcp_secrets(
    crud_store: &CrudStore,
    gateway_secrets: &GatewaySecrets,
    dry_run: bool,
) -> Result<McpSecretGarbageCollectionReport> {
    let crud_store = crud_store.with_maintenance_access();
    // Enumerate metadata first. Fresh owned refs/UUIDs created afterwards cannot
    // be this run's orphan candidates. One DB snapshot retains pending and
    // committed native owners without a gap between their atomic transitions.
    let stored_refs = gateway_secrets
        .list_mcp_secret_refs()
        .context("failed to list stored MCP secret refs for GC")?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let oauth = gateway_secrets.mcp_oauth_persistence();
    let oauth_ids = oauth
        .ids()
        .await
        .map_err(|_| anyhow::anyhow!("OAuth secret enumeration failed"))?;
    let retention = crud_store.plugin_credential_retention().await?;
    let mut active_refs = BTreeSet::new();
    let mut active_ids = std::collections::HashSet::new();
    for (kind, owner, payload) in retention {
        if kind == "native" {
            active_ids.insert(owner);
            let refs: Vec<McpSecretRef> =
                serde_json::from_str(&payload).context("failed to decode MCP secret refs")?;
            active_refs.extend(refs.into_iter().map(|entry| entry.ref_id));
        } else {
            anyhow::ensure!(
                payload.len() <= 65536,
                "pending credential retention exceeds bound"
            );
            let plan: serde_json::Value = serde_json::from_str(&payload)?;
            for child in plan
                .get("children")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if child.get("kind").and_then(serde_json::Value::as_str) == Some("mcp") {
                    if let Some(id) = child.get("id").and_then(serde_json::Value::as_str) {
                        active_ids.insert(id.to_owned());
                    }
                }
            }
            for field in ["cleanup_refs", "retained_refs"] {
                for entry in plan
                    .get(field)
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    active_refs.insert(
                        entry
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid pending credential reference"))?
                            .to_owned(),
                    );
                }
            }
        }
    }
    let oauth_stored = oauth_ids.len();
    let mut oauth_orphans = 0;
    let mut oauth_deleted = 0;
    if !oauth_ids.is_empty() {
        for id in oauth_ids {
            if !active_ids.contains(&id) {
                oauth_orphans += 1;
                if !dry_run {
                    oauth
                        .delete(&id)
                        .await
                        .map_err(|_| anyhow::anyhow!("OAuth orphan cleanup failed"))?;
                    oauth_deleted += 1;
                }
            }
        }
    }

    let orphan_refs = stored_refs
        .difference(&active_refs)
        .cloned()
        .collect::<Vec<_>>();

    let (deleted_refs, failed_deletes) = if dry_run {
        (0, Vec::new())
    } else {
        let report = gateway_secrets.delete_mcp_secrets(orphan_refs.iter().map(String::as_str));
        (report.deleted, report.failed)
    };

    Ok(McpSecretGarbageCollectionReport {
        oauth_stored,
        oauth_orphans,
        oauth_deleted,
        active_refs: active_refs.len(),
        stored_refs: stored_refs.len(),
        orphan_refs: orphan_refs.len(),
        deleted_refs,
        failed_deletes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{McpAuditEventRecord, McpServerInstallationRecord};
    use pioneer_keystore::{MemorySecretStore, SecretId, SecretKind, SecretMeta, SecretStore};
    use sea_orm::Database;
    use std::sync::Arc;

    // In-memory fixture only; no user's keystore.
    #[tokio::test]
    async fn interrupted_parent_retains_uncertain_refs_until_explicit_cleanup() {
        use sea_orm::{EntityTrait, Set};
        let (store, secrets) = setup_gc().await;
        let now = chrono::Utc::now().fixed_offset();
        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set("ws_gc".into()),
            name: Set("GC fixture".into()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&store.database_connection())
        .await
        .unwrap();
        let id = "P".repeat(21);
        let pending = serde_json::json!({"kind":"native","native_committed":false,
            "children":[{"kind":"mcp","id":"reserved_native"}],"cleanup_refs":["uncertain"]})
        .to_string();
        store
            .insert_plugin_installation(&pioneer_entity::plugin_installation::Model {
                id: id.clone(),
                workspace_id: "ws_gc".into(),
                name: "fixture.tools".into(),
                version: None,
                source_upload_id: "fixture".into(),
                package_path: "/managed/package".into(),
                data_path: "/managed/data".into(),
                package_fingerprint: "fixture".into(),
                enabled: true,
                state: "installing".into(),
                revision: 1,
                pending_json: Some(pending.clone()),
                last_error: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();
        store
            .interrupt_plugin_mutation(&id, 1, &pending, "fixture interruption")
            .await
            .unwrap();
        secrets
            .put_mcp_secret("uncertain", "fixture value", None)
            .unwrap();
        secrets
            .put_mcp_secret("orphan", "fixture orphan", None)
            .unwrap();
        let report = garbage_collection_orphan_mcp_secrets(&store, &secrets, false)
            .await
            .unwrap();
        assert_eq!(report.deleted_refs, 1);
        assert!(secrets.get_mcp_secret("uncertain").unwrap().is_some());
        assert!(secrets.get_mcp_secret("orphan").unwrap().is_none());
        assert_eq!(
            store
                .find_plugin_installation(&id)
                .await
                .unwrap()
                .unwrap()
                .pending_json
                .as_deref(),
            Some(pending.as_str())
        );
        secrets.delete_mcp_secret("uncertain").unwrap();
        store
            .finish_plugin_mutation(&id, 1, "installed", None)
            .await
            .unwrap();
        assert_eq!(
            garbage_collection_orphan_mcp_secrets(&store, &secrets, false)
                .await
                .unwrap()
                .active_refs,
            0
        );
    }

    #[tokio::test]
    async fn oauth_gc_keeps_active_registration_and_deletes_only_orphans_after_restart() {
        let (store, secrets) = setup_gc().await;
        seed_installation(&store, "native", vec![]).await;
        let id = store
            .find_mcp_server_installation("workspace", "ws_gc", "native")
            .await
            .unwrap()
            .unwrap()
            .id
            .unwrap();
        let persistence = secrets.mcp_oauth_persistence();
        let registration = pioneer_mcp_oauth::Registration {
            token_endpoint_auth_method: None,
            client_id: "client".into(),
            client_secret: Some("secret-canary".into()),
            redirect_uri: "http://127.0.0.1:37643/oauth/mcp/callback".into(),
            scopes: vec![],
            application_type: None,
            registration_request: None,
            registration_response: None,
        };
        let record = pioneer_mcp_oauth::AuthorizationRecord {
            identity: "hash".into(),
            resource: "https://resource.test/mcp".into(),
            issuer: "https://issuer.test".into(),
            registration,
            credentials: None,
            pending_consent: None,
        };
        persistence.write(&id, record.clone()).await.unwrap();
        persistence.write("orphan", record).await.unwrap();
        let dry = garbage_collection_orphan_mcp_secrets(&store, &secrets, true)
            .await
            .unwrap();
        assert_eq!(dry.oauth_orphans, 1);
        assert_eq!(dry.oauth_deleted, 0);
        let applied = garbage_collection_orphan_mcp_secrets(&store, &secrets, false)
            .await
            .unwrap();
        assert_eq!(applied.oauth_deleted, 1);
        assert!(persistence.read(&id).await.unwrap().is_some());
        assert!(persistence.read("orphan").await.unwrap().is_none());
        let again = garbage_collection_orphan_mcp_secrets(&store, &secrets, false)
            .await
            .unwrap();
        assert_eq!(again.oauth_orphans, 0);
    }

    #[tokio::test]
    async fn gc_dry_run_reports_orphans_without_deleting() {
        let (crud_store, gateway_secrets) = setup_gc().await;
        seed_installation(
            &crud_store,
            "resend",
            vec![secret_ref("active_ref", "env", "RESEND_API_KEY")],
        )
        .await;
        gateway_secrets
            .put_mcp_secret("active_ref", "active", None)
            .expect("put active");
        gateway_secrets
            .put_mcp_secret("orphan_ref", "orphan", None)
            .expect("put orphan");

        let report = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, true)
            .await
            .expect("dry-run GC");

        assert_eq!(report.active_refs, 1);
        assert_eq!(report.stored_refs, 2);
        assert_eq!(report.orphan_refs, 1);
        assert_eq!(report.deleted_refs, 0);
        assert!(report.failed_deletes.is_empty());
        assert_eq!(
            gateway_secrets
                .get_mcp_secret("orphan_ref")
                .expect("read orphan"),
            Some("orphan".to_owned())
        );
    }

    #[tokio::test]
    async fn gc_deletes_orphans_and_keeps_active_and_non_mcp_entries() {
        let (crud_store, gateway_secrets) = setup_gc().await;
        seed_installation(
            &crud_store,
            "resend",
            vec![secret_ref("active_ref", "env", "RESEND_API_KEY")],
        )
        .await;
        gateway_secrets
            .put_mcp_secret("active_ref", "active", None)
            .expect("put active");
        gateway_secrets
            .put_mcp_secret("orphan_ref", "orphan", None)
            .expect("put orphan");
        gateway_secrets
            .set_provider_api_key("openrouter", "sk-provider")
            .expect("put provider");

        let report = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, false)
            .await
            .expect("GC");

        assert_eq!(report.active_refs, 1);
        assert_eq!(report.stored_refs, 2);
        assert_eq!(report.orphan_refs, 1);
        assert_eq!(report.deleted_refs, 1);
        assert!(report.failed_deletes.is_empty());
        assert_eq!(
            gateway_secrets
                .get_mcp_secret("active_ref")
                .expect("read active"),
            Some("active".to_owned())
        );
        assert_eq!(
            gateway_secrets
                .get_mcp_secret("orphan_ref")
                .expect("read orphan"),
            None
        );
        assert_eq!(
            gateway_secrets
                .get_provider_api_key("openrouter")
                .expect("read provider"),
            Some("sk-provider".to_owned())
        );
    }

    #[tokio::test]
    async fn gc_fails_on_invalid_active_secret_refs_json() {
        let (crud_store, gateway_secrets) = setup_gc().await;
        let mut record = installation_record("broken", vec![]);
        record.secret_refs_json = "not-json".to_owned();
        persist_installation(&crud_store, record).await;

        let error = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, false)
            .await
            .expect_err("invalid refs should fail GC");
        assert!(
            format!("{error:#}").contains("failed to decode MCP secret refs"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn gc_reports_delete_failures() {
        let (crud_store, _gateway_secrets) = setup_gc().await;
        let failing_store = Arc::new(DeleteFailingStore::new());
        let gateway_secrets = GatewaySecrets::new(failing_store.clone());
        failing_store
            .put_string(
                &SecretId::mcp_secret("orphan_ref").expect("mcp id"),
                "orphan",
                SecretMeta::new(SecretKind::McpSecret, Some("orphan".to_owned()), 123),
            )
            .expect("seed failing store");

        let report = garbage_collection_orphan_mcp_secrets(&crud_store, &gateway_secrets, false)
            .await
            .expect("GC with delete failure");

        assert_eq!(report.orphan_refs, 1);
        assert_eq!(report.deleted_refs, 0);
        assert_eq!(report.failed_deletes.len(), 1);
        assert_eq!(report.failed_deletes[0].ref_id, "orphan_ref");
    }

    async fn setup_gc() -> (CrudStore, GatewaySecrets) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("connect sqlite");
        Migrator::up(&connection, None).await.expect("migrate");
        let crud_store = CrudStore::new(connection);
        let gateway_secrets = GatewaySecrets::new(Arc::new(MemorySecretStore::new()));
        (crud_store, gateway_secrets)
    }

    async fn seed_installation(crud_store: &CrudStore, name: &str, secret_refs: Vec<McpSecretRef>) {
        persist_installation(crud_store, installation_record(name, secret_refs)).await;
    }

    async fn persist_installation(crud_store: &CrudStore, record: McpServerInstallationRecord) {
        let audit = McpAuditEventRecord {
            turn_id: None,
            server_installation_id: None,
            server_name: record.name.clone(),
            raw_tool_name: None,
            callable_name: None,
            catalog_version: None,
            action: "install".to_owned(),
            decision: "allowed".to_owned(),
            reason_code: None,
            details_json: "{}".to_owned(),
            created_at_unix: 1_700_000_000,
        };
        crud_store
            .upsert_mcp_server_installation_with_audit(&record, &audit, 1_700_000_000)
            .await
            .expect("persist MCP installation");
    }

    fn installation_record(
        name: &str,
        secret_refs: Vec<McpSecretRef>,
    ) -> McpServerInstallationRecord {
        McpServerInstallationRecord {
            id: None,
            scope_kind: "workspace".to_owned(),
            scope_key: "ws_gc".to_owned(),
            name: name.to_owned(),
            display_name: None,
            source_kind: "config".to_owned(),
            source_ref: "{}".to_owned(),
            transport_kind: "stdio".to_owned(),
            transport_json: "{}".to_owned(),
            auth_json: "{}".to_owned(),
            secret_refs_json: serde_json::to_string(&secret_refs).expect("encode refs"),
            enabled: true,
            allow_implicit_invocation: true,
            required: false,
            fingerprint: format!("fp-{name}"),
            updated_at_unix: 0,
        }
    }

    fn secret_ref(ref_id: &str, source: &str, name: &str) -> McpSecretRef {
        McpSecretRef {
            ref_id: ref_id.to_owned(),
            source: source.to_owned(),
            name: name.to_owned(),
        }
    }

    struct DeleteFailingStore {
        inner: MemorySecretStore,
    }

    impl Default for DeleteFailingStore {
        fn default() -> Self {
            Self {
                inner: MemorySecretStore::new(),
            }
        }
    }

    impl DeleteFailingStore {
        fn new() -> Self {
            Self::default()
        }
    }

    impl SecretStore for DeleteFailingStore {
        fn get_string(&self, id: &SecretId) -> pioneer_keystore::Result<Option<String>> {
            self.inner.get_string(id)
        }

        fn put_string(
            &self,
            id: &SecretId,
            value: &str,
            meta: SecretMeta,
        ) -> pioneer_keystore::Result<()> {
            self.inner.put_string(id, value, meta)
        }

        fn delete(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            Err(pioneer_keystore::KeystoreError::DeleteFailed(format!(
                "delete failed for {}",
                id.user()
            )))
        }

        fn exists(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            self.inner.exists(id)
        }

        fn list(
            &self,
            filter: pioneer_keystore::SecretFilter,
        ) -> pioneer_keystore::Result<Vec<pioneer_keystore::SecretEntryMeta>> {
            self.inner.list(filter)
        }
    }
}
