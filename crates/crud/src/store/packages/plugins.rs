use crate::{CrudStore, PackageRecord};
use anyhow::Result;
use pioneer_crud_packages::package_store::PackageStore;
pub use pioneer_crud_packages::package_store::{
    PackageNativeWrite, PackageOwnershipWrite, PluginNativeWrite, PluginOwnershipWrite,
};
use pioneer_entity::package_component;
impl CrudStore {
    pub(crate) fn package_store(&self) -> PackageStore {
        PackageStore::new(self.connection.clone())
    }
    pub async fn skill_plugin_owners(
        &self,
        ids: &[String],
    ) -> Result<Vec<(package_component::Model, Option<PackageRecord>)>> {
        self.package_store().skill_plugin_owners(ids).await
    }

    pub async fn get_plugin_selection(
        &self,
        turn: &str,
    ) -> Result<Option<pioneer_protocol::PluginSelectionSnapshot>> {
        self.package_store().get_plugin_selection(turn).await
    }

    pub async fn prepare_plugin_selection(
        &self,
        turn: &str,
        snapshot: &pioneer_protocol::PluginSelectionSnapshot,
    ) -> Result<()> {
        self.package_store()
            .prepare_plugin_selection(turn, snapshot)
            .await
    }

    /// `resolved_skills` comes from the native resolver's committed event, not
    /// from the client or inferred from missing DB bindings.
    pub async fn ready_plugin_selection(
        &self,
        turn: &str,
        resolved_skills: &[pioneer_protocol::SkillId],
    ) -> Result<()> {
        self.package_store()
            .ready_plugin_selection(turn, resolved_skills)
            .await
    }

    pub async fn plugin_turn_child_available(
        &self,
        turn: &str,
        kind: &str,
        id: &str,
        workspace: &str,
    ) -> Result<bool> {
        self.package_store()
            .plugin_turn_child_available(turn, kind, id, workspace)
            .await
    }

    pub async fn reserve_plugin_installation(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
    ) -> Result<()> {
        self.package_store()
            .reserve_plugin_installation(parent, connection_id, now)
            .await
    }

    pub async fn reserve_plugin_installation_with_metadata(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        self.package_store()
            .reserve_plugin_installation_with_metadata(parent, connection_id, now, metadata)
            .await
    }

    pub async fn reserve_package_installation_with_metadata(
        &self,
        parent: &PackageRecord,
        connection_id: u64,
        now: i64,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        self.package_store()
            .reserve_package_installation_with_metadata(parent, connection_id, now, metadata)
            .await
    }

    pub async fn list_plugin_installations(&self, workspace: &str) -> Result<Vec<PackageRecord>> {
        self.package_store()
            .list_plugin_installations(workspace)
            .await
    }

    pub async fn list_package_installations(
        &self,
        workspace: &str,
        kind: crate::PackageKind,
    ) -> Result<Vec<PackageRecord>> {
        self.package_store()
            .list_package_installations(workspace, kind)
            .await
    }

    pub async fn settle_plugin_installation(
        &self,
        id: &str,
        revision: i64,
        state: &str,
        error: Option<String>,
    ) -> Result<()> {
        self.package_store()
            .settle_plugin_installation(id, revision, state, error)
            .await
    }

    pub async fn record_plugin_component_failure(
        &self,
        write: &PluginOwnershipWrite,
        kind: &str,
        diagnostic: &str,
    ) -> Result<()> {
        self.package_store()
            .record_plugin_component_failure(write, kind, diagnostic)
            .await
    }

    /// Reused at admission and late dispatch. An unfinished parent is closed
    /// even if the process died before it could mark the operation interrupted.
    pub async fn plugin_child_available(
        &self,
        kind: &str,
        child: &str,
        workspace: &str,
    ) -> Result<bool> {
        self.package_store()
            .plugin_child_available(kind, child, workspace)
            .await
    }

    /// Native MCP sessions may be recreated during the final reload while
    /// execution is still fenced by the retained parent pending plan. This
    /// check is only for process admission, never tool/capability selection.
    pub async fn plugin_child_runtime_available(
        &self,
        child: &str,
        workspace: &str,
    ) -> Result<bool> {
        self.package_store()
            .plugin_child_runtime_available(child, workspace)
            .await
    }

    pub async fn prepare_plugin_reload(&self, id: &str, revision: i64) -> Result<()> {
        self.package_store()
            .prepare_plugin_reload(id, revision)
            .await
    }

    pub async fn insert_plugin_installation(&self, parent: &PackageRecord) -> Result<()> {
        self.package_store()
            .insert_plugin_installation(parent)
            .await
    }

    pub async fn insert_package_installation(&self, parent: &PackageRecord) -> Result<()> {
        self.package_store()
            .insert_package_installation(parent)
            .await
    }

    /// Scalars and ordered lists are read from one bounded physical reader snapshot.
    /// Mapping runs after release; writers cannot expose mixed metadata generations.
    pub async fn plugin_metadata_inventory(
        &self,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, (i64, pioneer_protocol::PluginMetadata)>> {
        self.package_store().plugin_metadata_inventory(ids).await
    }

    pub async fn plugin_metadata(
        &self,
        parent: &PackageRecord,
    ) -> Result<Option<pioneer_protocol::PluginMetadata>> {
        self.package_store().plugin_metadata(parent).await
    }

    pub async fn find_plugin_installation(&self, id: &str) -> Result<Option<PackageRecord>> {
        self.package_store().find_plugin_installation(id).await
    }

    pub async fn find_package_installation(&self, id: &str) -> Result<Option<PackageRecord>> {
        self.package_store().find_package_installation(id).await
    }

    pub async fn find_plugin_by_upload(&self, upload: &str) -> Result<Option<PackageRecord>> {
        self.package_store().find_plugin_by_upload(upload).await
    }

    pub async fn plugin_components_inventory(
        &self,
        ids: &[String],
    ) -> Result<Vec<package_component::Model>> {
        self.package_store().plugin_components_inventory(ids).await
    }

    pub async fn list_plugin_components(&self, id: &str) -> Result<Vec<package_component::Model>> {
        self.package_store().list_plugin_components(id).await
    }

    pub async fn find_skill_plugin_owner(
        &self,
        id: &pioneer_protocol::SkillId,
    ) -> Result<Option<package_component::Model>> {
        self.package_store().find_skill_plugin_owner(id).await
    }

    pub async fn find_mcp_plugin_owner(
        &self,
        id: &str,
    ) -> Result<Option<package_component::Model>> {
        self.package_store().find_mcp_plugin_owner(id).await
    }

    pub async fn begin_plugin_mutation(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        state: &str,
        enabled: bool,
        pending: &str,
    ) -> Result<PackageRecord> {
        self.package_store()
            .begin_plugin_mutation(workspace, id, revision, state, enabled, pending)
            .await
    }

    pub async fn begin_plugin_mutation_with_policy(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        state: &str,
        enabled: bool,
        pending: &str,
        allow_implicit_invocation: Option<bool>,
    ) -> Result<PackageRecord> {
        self.package_store()
            .begin_plugin_mutation_with_policy(
                workspace,
                id,
                revision,
                state,
                enabled,
                pending,
                allow_implicit_invocation,
            )
            .await
    }

    pub async fn finish_plugin_mutation(
        &self,
        id: &str,
        revision: i64,
        state: &str,
        error: Option<String>,
    ) -> Result<()> {
        self.package_store()
            .finish_plugin_mutation(id, revision, state, error)
            .await
    }

    pub async fn replace_plugin_pending(
        &self,
        id: &str,
        revision: i64,
        pending: &str,
    ) -> Result<()> {
        self.package_store()
            .replace_plugin_pending(id, revision, pending)
            .await
    }

    pub async fn delete_plugin_parent(&self, id: &str, revision: i64) -> Result<()> {
        self.package_store()
            .delete_plugin_parent(id, revision)
            .await
    }

    pub async fn interrupt_unfinished_plugins(&self) -> Result<()> {
        self.package_store().interrupt_unfinished_plugins().await
    }

    pub async fn plugin_workspace_native_threads(
        &self,
        workspace: &str,
        ids: &[String],
    ) -> Result<Vec<String>> {
        self.package_store()
            .plugin_workspace_native_threads(workspace, ids)
            .await
    }

    /// Restore the still-owned bounded action after settle/reload uncertainty.
    /// Request cancellation outside this catch leaves the prior closed plan.
    pub async fn interrupt_plugin_mutation(
        &self,
        id: &str,
        revision: i64,
        pending: &str,
        error: &str,
    ) -> Result<()> {
        self.package_store()
            .interrupt_plugin_mutation(id, revision, pending, error)
            .await
    }

    pub async fn plugin_graph_stop_candidates(
        &self,
        workspace: &str,
        parent: &str,
    ) -> Result<Vec<(String, String)>> {
        self.package_store()
            .plugin_graph_stop_candidates(workspace, parent)
            .await
    }

    /// Publish an existing OAuth operation's completed effect; the OAuth
    /// engine/persistence remains authoritative for credentials and cleanup.
    pub async fn complete_plugin_native_effect(
        &self,
        write: &PluginNativeWrite,
        workspace: &str,
        kind: &str,
    ) -> Result<()> {
        self.package_store()
            .complete_plugin_native_effect(write, workspace, kind)
            .await
    }

    pub async fn plugin_credential_retention(&self) -> Result<Vec<(String, String, String)>> {
        self.package_store().plugin_credential_retention().await
    }

    pub async fn resume_plugin_mutation(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        expected_pending: &str,
        pending: &str,
        state: &str,
    ) -> Result<PackageRecord> {
        self.package_store()
            .resume_plugin_mutation(workspace, id, revision, expected_pending, pending, state)
            .await
    }

    pub async fn begin_plugin_package_update(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
        pending: &str,
        upload_id: &str,
        connection_id: u64,
        now: i64,
        previous_pending: Option<&str>,
    ) -> Result<PackageRecord> {
        self.package_store()
            .begin_plugin_package_update(
                workspace,
                id,
                revision,
                pending,
                upload_id,
                connection_id,
                now,
                previous_pending,
            )
            .await
    }

    pub async fn publish_plugin_package(
        &self,
        id: &str,
        revision: i64,
        name: &str,
        version: Option<&str>,
        fingerprint: &str,
        upload_id: Option<&str>,
    ) -> Result<()> {
        self.package_store()
            .publish_plugin_package(id, revision, name, version, fingerprint, upload_id)
            .await
    }

    pub async fn publish_plugin_package_with_metadata(
        &self,
        id: &str,
        revision: i64,
        name: &str,
        version: Option<&str>,
        fingerprint: &str,
        upload_id: Option<&str>,
        metadata: Option<&pioneer_protocol::PluginMetadata>,
    ) -> Result<()> {
        self.package_store()
            .publish_plugin_package_with_metadata(
                id,
                revision,
                name,
                version,
                fingerprint,
                upload_id,
                metadata,
            )
            .await
    }

    pub async fn forget_plugin_component(
        &self,
        id: &str,
        revision: i64,
        kind: &str,
        key: &str,
    ) -> Result<()> {
        self.package_store()
            .forget_plugin_component(id, revision, kind, key)
            .await
    }

    pub async fn publish_app_package(
        &self,
        id: &str,
        revision: i64,
        upload_id: Option<&str>,
        metadata: &crate::PreparedAppMetadata,
    ) -> Result<()> {
        self.package_store()
            .publish_app_package(id, revision, upload_id, metadata)
            .await
    }

    pub async fn native_package_owner(
        &self,
        kind: &str,
        child: &str,
    ) -> Result<Option<pioneer_protocol::PackageOwner>> {
        self.package_store().native_package_owner(kind, child).await
    }

    pub async fn app_runtime_page(
        &self,
        after: &str,
        through: &str,
    ) -> Result<Vec<(String, Option<PackageRecord>)>> {
        self.package_store().app_runtime_page(after, through).await
    }

    pub async fn app_runtime_frontier(&self) -> Result<Option<String>> {
        self.package_store().app_runtime_frontier().await
    }
}
