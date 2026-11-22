use crate::{
    CrudStore, McpAuditEventRecord, McpServerCatalogSnapshotRecord, McpServerInstallationRecord,
    PluginNativeWrite, PluginOwnershipWrite, SkillAuditEventRecord, SkillDependencySnapshotRecord,
    SkillInstallationPatch, SkillInstallationRecord, SkillPackChildDiff,
    SkillPackInstallationRecord, SkillReconciliationSnapshot, SkillUploadSessionRecord,
    TurnMcpBindingRecord, TurnMcpProjectionPersistenceError, TurnMcpProjectionRecord,
    TurnMcpProjectionReplaceOutcome, TurnMcpProjectionReplacement, TurnSkillBindingRecord,
    WorkspaceSkillPolicyRecord,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::policy;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_packages::skill_pack_transactions::SkillPackRepository;
use pioneer_crud_packages::turn_skill_binding;
use pioneer_protocol::{SkillId, SkillPackId};
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn replace_turn_skill_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnSkillBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .replace_turn_skill_bindings(turn_id, bindings, event_timestamp_secs)
            .await
    }

    pub async fn replace_turn_skill_bindings_with_authorization_context(
        &self,
        turn_id: &str,
        bindings: &[TurnSkillBindingRecord],
        event_timestamp_secs: i64,
        authorization_context_json: &str,
    ) -> Result<()> {
        let turn_id = turn_id.to_owned();
        let prepared = turn_skill_binding::prepare_turn_skill_bindings(
            turn_id.as_str(),
            bindings,
            unix_to_datetime(event_timestamp_secs),
        );
        let authorization_context_json = authorization_context_json.to_owned();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin authorized turn skill projection transaction")?;
            turn_skill_binding::replace_prepared_turn_skill_bindings(
                &transaction,
                prepared.clone(),
            )
            .await?;
            if !pioneer_crud_execution::turn_envelope::set_turn_execution_authorization_context(
                &transaction,
                turn_id.as_str(),
                authorization_context_json.as_str(),
            )
            .await?
            {
                anyhow::bail!("turn disappeared while binding its skill projection");
            }
            transaction
                .commit()
                .await
                .context("failed to commit authorized turn skill projection transaction")
        })
        .await
    }

    pub async fn list_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        self.package_store().list_turn_skill_bindings(turn_id).await
    }

    pub async fn find_turn_skill_bindings(
        &self,
        turn_id: &str,
    ) -> Result<Vec<TurnSkillBindingRecord>> {
        self.package_store().find_turn_skill_bindings(turn_id).await
    }

    pub async fn list_workspace_skill_policies(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceSkillPolicyRecord>> {
        self.package_store()
            .list_workspace_skill_policies(workspace_id)
            .await
    }

    pub async fn find_workspace_skill_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<Option<WorkspaceSkillPolicyRecord>> {
        self.package_store()
            .find_workspace_skill_policy(workspace_id, skill_id)
            .await
    }

    pub async fn upsert_workspace_skill_policy(
        &self,
        record: &WorkspaceSkillPolicyRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .upsert_workspace_skill_policy(record, event_timestamp_secs)
            .await
    }

    pub async fn upsert_workspace_skill_policy_with_plugin_change(
        &self,
        record: &WorkspaceSkillPolicyRecord,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .upsert_workspace_skill_policy_with_plugin_change(record, native, event_timestamp_secs)
            .await
    }

    pub async fn delete_workspace_skill_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<bool> {
        self.package_store()
            .delete_workspace_skill_policy(workspace_id, skill_id)
            .await
    }

    pub async fn delete_workspace_skill_policy_with_plugin_change(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
        native: Option<&PluginNativeWrite>,
    ) -> Result<bool> {
        self.package_store()
            .delete_workspace_skill_policy_with_plugin_change(workspace_id, skill_id, native)
            .await
    }

    pub async fn insert_skill_installation(
        &self,
        record: &SkillInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .insert_skill_installation(record, event_timestamp_secs)
            .await
    }

    pub async fn insert_skill_installation_with_policy(
        &self,
        installation: &SkillInstallationRecord,
        policy: &WorkspaceSkillPolicyRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .insert_skill_installation_with_policy(installation, policy, event_timestamp_secs)
            .await
    }

    pub async fn install_skill_lifecycle(
        &self,
        installation: &SkillInstallationRecord,
        policy: &WorkspaceSkillPolicyRecord,
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.install_skill_lifecycle_with_ownership(
            installation,
            policy,
            audit_records,
            Some(upload_id),
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn install_skill_lifecycle_with_ownership(
        &self,
        installation: &SkillInstallationRecord,
        policy: &WorkspaceSkillPolicyRecord,
        audit_records: &[SkillAuditEventRecord],
        upload_id: Option<&str>,
        ownership: Option<&PluginOwnershipWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.package_store()
            .install_skill_lifecycle_with_ownership(
                installation,
                policy,
                audit_records,
                upload_id,
                ownership,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn update_skill_installation(
        &self,
        skill_id: &SkillId,
        patch: &SkillInstallationPatch,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        pioneer_crud_packages::skill_reconciliation::update_skill_installation(
            &self.connection,
            skill_id,
            patch,
            event_timestamp_secs,
        )
        .await
    }

    /// Reconciliation validates the row used for FS preparation, not a later reread.
    pub async fn reconcile_skill_installation(
        &self,
        expected: &SkillReconciliationSnapshot,
        patch: &SkillInstallationPatch,
        now: i64,
        workspace: Option<&pioneer_entity::workspace::Model>,
        still_current: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<bool> {
        self.reconcile_skill_installation_with_progress(
            expected,
            patch,
            now,
            workspace,
            still_current,
            &std::sync::atomic::AtomicBool::new(false),
        )
        .await
    }

    pub async fn reconcile_skill_installation_with_progress(
        &self,
        expected: &SkillReconciliationSnapshot,
        patch: &SkillInstallationPatch,
        now: i64,
        workspace: Option<&pioneer_entity::workspace::Model>,
        still_current: &(dyn Fn() -> bool + Send + Sync),
        mutation_started: &std::sync::atomic::AtomicBool,
    ) -> Result<bool> {
        pioneer_crud_packages::skill_reconciliation::reconcile_skill_installation_with_progress(
            &self.connection,
            expected,
            patch,
            now,
            workspace,
            still_current,
            mutation_started,
        )
        .await
    }

    /// Resolve an uncertain publication against current full facts without a
    /// second metadata UPDATE/WAL write. Uses the same short writer guards.
    pub async fn confirm_skill_reconciliation(
        &self,
        expected: &SkillReconciliationSnapshot,
        workspace: Option<&pioneer_entity::workspace::Model>,
        still_current: &(dyn Fn() -> bool + Send + Sync),
        transaction_started: &std::sync::atomic::AtomicBool,
    ) -> Result<bool> {
        pioneer_crud_packages::skill_reconciliation::confirm_skill_reconciliation(
            &self.connection,
            expected,
            workspace,
            still_current,
            transaction_started,
        )
        .await
    }

    pub async fn update_skill_lifecycle(
        &self,
        skill_id: &SkillId,
        patch: &SkillInstallationPatch,
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.update_skill_lifecycle_with_ownership(
            skill_id,
            patch,
            audit_records,
            Some(upload_id),
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn update_skill_lifecycle_with_ownership(
        &self,
        skill_id: &SkillId,
        patch: &SkillInstallationPatch,
        audit_records: &[SkillAuditEventRecord],
        upload_id: Option<&str>,
        ownership: Option<&PluginOwnershipWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.update_skill_lifecycle_with_plugin_change(
            skill_id,
            patch,
            audit_records,
            upload_id,
            ownership,
            None,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn update_skill_lifecycle_with_plugin_change(
        &self,
        skill_id: &SkillId,
        patch: &SkillInstallationPatch,
        audit_records: &[SkillAuditEventRecord],
        upload_id: Option<&str>,
        ownership: Option<&PluginOwnershipWrite>,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.package_store()
            .update_skill_lifecycle_with_plugin_change(
                skill_id,
                patch,
                audit_records,
                upload_id,
                ownership,
                native,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn delete_skill_installation(&self, skill_id: &SkillId) -> Result<bool> {
        self.package_store()
            .delete_skill_installation(skill_id)
            .await
    }

    pub async fn delete_skill_installation_with_workspace_policy(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<bool> {
        self.package_store()
            .delete_skill_installation_with_workspace_policy(workspace_id, skill_id)
            .await
    }

    pub async fn find_skill_installation(
        &self,
        skill_id: &SkillId,
    ) -> Result<Option<SkillInstallationRecord>> {
        self.package_store().find_skill_installation(skill_id).await
    }

    pub async fn list_active_workspace_page(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<pioneer_entity::workspace::Model>> {
        pioneer_crud_access::membership::list_active_workspace_page(&self.connection, after).await
    }

    /// Pending import registration is fenced by the same Workspace snapshot as
    /// its later projection. System imports have no Workspace owner.
    pub async fn register_skill_import_pending(
        &self,
        record: &SkillInstallationRecord,
        workspace: Option<&pioneer_entity::workspace::Model>,
        now: i64,
    ) -> Result<Option<SkillReconciliationSnapshot>> {
        pioneer_crud_packages::skill_reconciliation::register_skill_import_pending(
            &self.connection,
            record,
            workspace,
            now,
        )
        .await
    }

    pub async fn find_skill_import_provenance(
        &self,
        kind: &str,
        scope: &str,
        reference: &str,
    ) -> Result<Option<SkillReconciliationSnapshot>> {
        pioneer_crud_packages::skill_reconciliation::find_skill_import_provenance(
            &self.connection,
            kind,
            scope,
            reference,
        )
        .await
    }

    pub async fn list_skill_reconciliation_page(
        &self,
        source_kind: &str,
        scope_key: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<SkillReconciliationSnapshot>> {
        pioneer_crud_packages::skill_reconciliation::list_skill_reconciliation_page(
            &self.connection,
            source_kind,
            scope_key,
            after,
            limit,
        )
        .await
    }

    pub async fn list_skill_installations_scope_page(
        &self,
        source_kind: &str,
        scope_key: &str,
        after: Option<&str>,
        limit: u64,
    ) -> Result<Vec<SkillInstallationRecord>> {
        self.package_store()
            .list_skill_installations_scope_page(source_kind, scope_key, after, limit)
            .await
    }

    pub async fn list_skill_installations(&self) -> Result<Vec<SkillInstallationRecord>> {
        self.package_store().list_skill_installations().await
    }

    pub async fn insert_skill_pack_installation(
        &self,
        record: &SkillPackInstallationRecord,
    ) -> Result<()> {
        self.package_store()
            .insert_skill_pack_installation(record)
            .await
    }

    pub async fn find_skill_pack_installation(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<Option<SkillPackInstallationRecord>> {
        let repository = SkillPackRepository::new(&self.connection);
        repository
            .find_skill_pack_installation(scope_key, pack_id)
            .await
    }

    pub async fn find_skill_pack_installation_by_id(
        &self,
        pack_id: &SkillPackId,
    ) -> Result<Option<SkillPackInstallationRecord>> {
        let repository = SkillPackRepository::new(&self.connection);
        repository.find_skill_pack_installation_by_id(pack_id).await
    }

    pub async fn list_skill_pack_installations(
        &self,
        scope_key: &str,
    ) -> Result<Vec<SkillPackInstallationRecord>> {
        let repository = SkillPackRepository::new(&self.connection);
        repository.list_skill_pack_installations(scope_key).await
    }

    pub async fn list_skill_installations_for_pack(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<Vec<SkillInstallationRecord>> {
        let repository = SkillPackRepository::new(&self.connection);
        repository
            .list_skill_installations_for_pack(scope_key, pack_id)
            .await
    }

    pub async fn update_skill_pack_installation_name(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
        name: &str,
        updated_at_unix: i64,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .update_skill_pack_installation_name(scope_key, pack_id, name, updated_at_unix)
            .await
    }

    pub async fn delete_skill_pack_installation(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .delete_skill_pack_installation(scope_key, pack_id)
            .await
    }

    pub async fn insert_skill_pack_installation_with_children(
        &self,
        parent: &SkillPackInstallationRecord,
        children: &[SkillInstallationRecord],
    ) -> Result<()> {
        SkillPackRepository::new(&self.connection)
            .insert_skill_pack_installation_with_children(parent, children)
            .await
    }

    pub async fn install_skill_pack_lifecycle(
        &self,
        parent: &SkillPackInstallationRecord,
        children: &[SkillInstallationRecord],
        policies: &[WorkspaceSkillPolicyRecord],
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .install_skill_pack_lifecycle(
                parent,
                children,
                policies,
                audit_records,
                upload_id,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn update_skill_pack_installation_with_child_diff(
        &self,
        parent: &SkillPackInstallationRecord,
        diff: &SkillPackChildDiff,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .update_skill_pack_installation_with_child_diff(parent, diff)
            .await
    }

    pub async fn update_skill_pack_lifecycle(
        &self,
        expected_parent: &SkillPackInstallationRecord,
        expected_children: &[SkillInstallationRecord],
        parent: &SkillPackInstallationRecord,
        diff: &SkillPackChildDiff,
        added_policies: &[WorkspaceSkillPolicyRecord],
        audit_records: &[SkillAuditEventRecord],
        upload_id: &str,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .update_skill_pack_lifecycle(
                expected_parent,
                expected_children,
                parent,
                diff,
                added_policies,
                audit_records,
                upload_id,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn uninstall_skill_pack_lifecycle(
        &self,
        expected_parent: &SkillPackInstallationRecord,
        expected_children: &[SkillInstallationRecord],
        audit_records: &[SkillAuditEventRecord],
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .uninstall_skill_pack_lifecycle(expected_parent, expected_children, audit_records)
            .await
    }

    pub async fn uninstall_skill_installation_lifecycle(
        &self,
        expected: &SkillInstallationRecord,
        audit_records: &[SkillAuditEventRecord],
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.package_store()
            .uninstall_skill_installation_lifecycle(expected, audit_records, event_timestamp_secs)
            .await
    }

    pub async fn uninstall_skill_installation_lifecycle_with_plugin_change(
        &self,
        expected: &SkillInstallationRecord,
        audit_records: &[SkillAuditEventRecord],
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        self.package_store()
            .uninstall_skill_installation_lifecycle_with_plugin_change(
                expected,
                audit_records,
                native,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn delete_skill_pack_installation_with_children(
        &self,
        scope_key: &str,
        pack_id: &SkillPackId,
    ) -> Result<bool> {
        SkillPackRepository::new(&self.connection)
            .delete_skill_pack_installation_with_children(scope_key, pack_id)
            .await
    }

    pub async fn insert_skill_upload_session(
        &self,
        record: &SkillUploadSessionRecord,
    ) -> Result<()> {
        self.package_store()
            .insert_skill_upload_session(record)
            .await
    }

    pub async fn find_skill_upload_session(
        &self,
        upload_id: &str,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.package_store()
            .find_skill_upload_session(upload_id)
            .await
    }

    pub async fn update_skill_upload_received_bytes(
        &self,
        upload_id: &str,
        received_bytes: u64,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.package_store()
            .update_skill_upload_received_bytes(upload_id, received_bytes, updated_at_unix)
            .await
    }

    pub async fn transition_skill_upload_status(
        &self,
        upload_id: &str,
        expected_statuses: &[&str],
        status: &str,
        finalized_at_unix: Option<i64>,
        consumed_at_unix: Option<i64>,
        aborted_at_unix: Option<i64>,
        updated_at_unix: i64,
    ) -> Result<Option<SkillUploadSessionRecord>> {
        self.package_store()
            .transition_skill_upload_status(
                upload_id,
                expected_statuses,
                status,
                finalized_at_unix,
                consumed_at_unix,
                aborted_at_unix,
                updated_at_unix,
            )
            .await
    }

    pub async fn list_expired_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        self.package_store()
            .list_expired_skill_upload_sessions(now_unix)
            .await
    }

    pub async fn list_stale_skill_upload_sessions(
        &self,
        now_unix: i64,
    ) -> Result<Vec<SkillUploadSessionRecord>> {
        self.package_store()
            .list_stale_skill_upload_sessions(now_unix)
            .await
    }

    pub async fn upsert_mcp_server_installation(
        &self,
        record: &McpServerInstallationRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.package_store()
            .upsert_mcp_server_installation(record, event_timestamp_secs)
            .await
    }

    pub async fn upsert_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.package_store()
            .upsert_mcp_server_installation_with_audit(record, audit, event_timestamp_secs)
            .await
    }

    pub async fn upsert_mcp_server_installation_with_audit_and_ownership(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        ownership: Option<&PluginOwnershipWrite>,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.package_store()
            .upsert_mcp_server_installation_with_audit_and_ownership(
                record,
                audit,
                ownership,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn upsert_mcp_server_installation_with_plugin_native_change(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        ownership: Option<&PluginOwnershipWrite>,
        native: Option<&PluginNativeWrite>,
        event_timestamp_secs: i64,
    ) -> Result<String> {
        self.package_store()
            .upsert_mcp_server_installation_with_plugin_native_change(
                record,
                audit,
                ownership,
                native,
                event_timestamp_secs,
            )
            .await
    }

    pub async fn list_mcp_server_installations(
        &self,
        scope_kind: &str,
        scope_key: &str,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        self.package_store()
            .list_mcp_server_installations(scope_kind, scope_key)
            .await
    }

    pub async fn list_all_mcp_server_installations(
        &self,
    ) -> Result<Vec<McpServerInstallationRecord>> {
        self.package_store()
            .list_all_mcp_server_installations()
            .await
    }

    pub async fn find_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        self.package_store()
            .find_mcp_server_installation(scope_kind, scope_key, name)
            .await
    }

    pub async fn find_mcp_server_installation_by_id(
        &self,
        scope_kind: &str,
        scope_key: &str,
        id: &str,
    ) -> Result<Option<McpServerInstallationRecord>> {
        self.package_store()
            .find_mcp_server_installation_by_id(scope_kind, scope_key, id)
            .await
    }

    pub async fn delete_mcp_server_installation(
        &self,
        scope_kind: &str,
        scope_key: &str,
        name: &str,
    ) -> Result<()> {
        self.package_store()
            .delete_mcp_server_installation(scope_kind, scope_key, name)
            .await
    }

    pub async fn delete_mcp_server_installation_with_audit(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
    ) -> Result<()> {
        self.package_store()
            .delete_mcp_server_installation_with_audit(record, audit)
            .await
    }

    pub async fn delete_mcp_server_installation_with_plugin_change(
        &self,
        record: &McpServerInstallationRecord,
        audit: &McpAuditEventRecord,
        native: Option<&PluginNativeWrite>,
    ) -> Result<()> {
        self.package_store()
            .delete_mcp_server_installation_with_plugin_change(record, audit, native)
            .await
    }

    pub async fn upsert_mcp_server_catalog_snapshot(
        &self,
        record: &McpServerCatalogSnapshotRecord,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .upsert_mcp_server_catalog_snapshot(record, event_timestamp_secs)
            .await
    }

    pub async fn find_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<Option<McpServerCatalogSnapshotRecord>> {
        self.package_store()
            .find_mcp_server_catalog_snapshot(server_installation_id)
            .await
    }

    pub async fn delete_mcp_server_catalog_snapshot(
        &self,
        server_installation_id: &str,
    ) -> Result<u64> {
        self.package_store()
            .delete_mcp_server_catalog_snapshot(server_installation_id)
            .await
    }

    pub async fn insert_mcp_audit_event_record(&self, record: &McpAuditEventRecord) -> Result<()> {
        self.package_store()
            .insert_mcp_audit_event_record(record)
            .await
    }

    pub async fn list_recent_mcp_audit_event_records(
        &self,
        server_name: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        self.package_store()
            .list_recent_mcp_audit_event_records(server_name, limit)
            .await
    }

    pub async fn list_recent_mcp_audit_event_records_for_server_id(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<McpAuditEventRecord>> {
        self.package_store()
            .list_recent_mcp_audit_event_records_for_server_id(server_installation_id, limit)
            .await
    }

    pub async fn replace_turn_mcp_bindings(
        &self,
        turn_id: &str,
        bindings: &[TurnMcpBindingRecord],
        event_timestamp_secs: i64,
    ) -> Result<()> {
        self.package_store()
            .replace_turn_mcp_bindings(turn_id, bindings, event_timestamp_secs)
            .await
    }

    /// Atomically persists one complete turn MCP projection. This is the only
    /// repository entry point callers need for a projection header plus zero/N bindings.
    pub async fn replace_turn_mcp_projection(
        &self,
        replacement: &TurnMcpProjectionReplacement,
    ) -> std::result::Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
    {
        self.package_store()
            .replace_turn_mcp_projection(replacement)
            .await
    }

    pub async fn replace_turn_mcp_projection_with_authorization_context(
        &self,
        replacement: &TurnMcpProjectionReplacement,
        authorization_context_json: &str,
    ) -> std::result::Result<TurnMcpProjectionReplaceOutcome, TurnMcpProjectionPersistenceError>
    {
        self.package_store()
            .replace_turn_mcp_projection_with_authorization_context(
                replacement,
                authorization_context_json,
            )
            .await
    }

    pub async fn list_turn_mcp_bindings(&self, turn_id: &str) -> Result<Vec<TurnMcpBindingRecord>> {
        self.package_store().list_turn_mcp_bindings(turn_id).await
    }

    pub async fn get_turn_mcp_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnMcpProjectionRecord>> {
        self.package_store().get_turn_mcp_projection(turn_id).await
    }

    pub async fn list_recent_turn_mcp_bindings_for_server(
        &self,
        server_installation_id: &str,
        limit: u64,
    ) -> Result<Vec<TurnMcpBindingRecord>> {
        self.package_store()
            .list_recent_turn_mcp_bindings_for_server(server_installation_id, limit)
            .await
    }

    pub async fn insert_skill_audit_event_records(
        &self,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.package_store()
            .insert_skill_audit_event_records(records)
            .await
    }

    pub async fn append_skill_audit_event_records(
        &self,
        turn_id: &str,
        records: &[SkillAuditEventRecord],
    ) -> Result<()> {
        self.package_store()
            .append_skill_audit_event_records(turn_id, records)
            .await
    }

    pub async fn persist_native_skill_audit_bundle(
        &self,
        turn_id: &str,
        records: &[SkillAuditEventRecord],
        dependency_snapshots: &[SkillDependencySnapshotRecord],
    ) -> Result<()> {
        pioneer_crud_packages::native_skill_audit_bundle::persist_native_skill_audit_bundle(
            &self.connection,
            turn_id,
            records,
            dependency_snapshots,
        )
        .await
    }

    pub async fn list_turn_skill_audit_event_records(
        &self,
        turn_id: &str,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        pioneer_crud_packages::native_skill_audit_bundle::list_turn_skill_audit_event_records(
            &self.connection,
            turn_id,
        )
        .await
    }

    pub async fn list_skill_audit_event_records(
        &self,
        skill_id: &SkillId,
        limit: u64,
    ) -> Result<Vec<SkillAuditEventRecord>> {
        self.package_store()
            .list_skill_audit_event_records(skill_id, limit)
            .await
    }

    pub async fn insert_skill_dependency_snapshot_record(
        &self,
        record: &SkillDependencySnapshotRecord,
    ) -> Result<()> {
        self.package_store()
            .insert_skill_dependency_snapshot_record(record)
            .await
    }

    pub async fn list_turn_skill_dependency_snapshot_records(
        &self,
        turn_id: &str,
    ) -> Result<Vec<SkillDependencySnapshotRecord>> {
        self.package_store()
            .list_turn_skill_dependency_snapshot_records(turn_id)
            .await
    }
}
