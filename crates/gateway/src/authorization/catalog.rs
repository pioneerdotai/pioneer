//! Machine-readable inventory of authorization-sensitive ingress.
//!
//! Dispatch still uses the typed RPC/binary registries directly. This
//! projection joins those registries with restricted authentication methods
//! and internal tools so tests can reject a new side-effecting entry point
//! until its action/resource contract is registered.

use pioneer_protocol::constants::methods;
use serde::Serialize;

use super::registry::{BINARY_INGRESS_REGISTRY, NORMAL_METHOD_REGISTRY};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthorizationCatalogSurface {
    Rpc,
    RestrictedRpc,
    BinaryIngress,
    InternalTool,
    DynamicToolFamily,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AuthorizationCatalogRecord {
    pub(crate) surface: AuthorizationCatalogSurface,
    pub(crate) entry_point: &'static str,
    pub(crate) action: &'static str,
    pub(crate) resource: &'static str,
    pub(crate) disclosure: &'static str,
    pub(crate) audit: &'static str,
    pub(crate) reauthorize_before_side_effect: bool,
}

const fn record(
    surface: AuthorizationCatalogSurface,
    entry_point: &'static str,
    action: &'static str,
    resource: &'static str,
    disclosure: &'static str,
    audit: &'static str,
    reauthorize_before_side_effect: bool,
) -> AuthorizationCatalogRecord {
    AuthorizationCatalogRecord {
        surface,
        entry_point,
        action,
        resource,
        disclosure,
        audit,
        reauthorize_before_side_effect,
    }
}

const RESTRICTED_RPC_RECORDS: &[AuthorizationCatalogRecord] = &[
    record(
        AuthorizationCatalogSurface::RestrictedRpc,
        methods::AUTH_REFRESH,
        "session_refresh",
        "refresh_credential",
        "authentication_terminal",
        "authentication",
        true,
    ),
    record(
        AuthorizationCatalogSurface::RestrictedRpc,
        methods::AUTH_DEVICE_ACTIVATE,
        "device_activate",
        "activation_credential",
        "authentication_terminal",
        "authentication",
        true,
    ),
    record(
        AuthorizationCatalogSurface::RestrictedRpc,
        methods::INVITE_PREVIEW,
        "restricted_invitation_preview",
        "invitation_credential",
        "not_found",
        "authentication",
        true,
    ),
    record(
        AuthorizationCatalogSurface::RestrictedRpc,
        methods::INVITE_ACCEPT,
        "restricted_invitation_accept",
        "invitation_credential",
        "not_found",
        "mutation",
        true,
    ),
];

const INTERNAL_TOOL_RECORDS: &[AuthorizationCatalogRecord] = &[
    record(
        AuthorizationCatalogSurface::InternalTool,
        "exec_command",
        "tool_shell_execute",
        "execution_workspace",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "write_stdin",
        "tool_shell_execute",
        "execution_session",
        "not_found",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "read_file",
        "tool_filesystem_read",
        "execution_workspace",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "write_file",
        "tool_filesystem_write",
        "execution_workspace",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "edit_file",
        "tool_filesystem_write",
        "execution_workspace",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "list_dir",
        "tool_filesystem_read",
        "execution_workspace",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "grep_files",
        "tool_filesystem_read",
        "execution_workspace",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "apply_patch",
        "tool_filesystem_write",
        "execution_workspace",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "web_search",
        "tool_network_use",
        "execution_network_policy",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "web_fetch",
        "tool_network_use",
        "execution_network_policy",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "download_url",
        "tool_network_use",
        "execution_network_policy",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "request_tools",
        "tool_bundle_expand",
        "execution_authority",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "computer_use",
        "tool_computer_use",
        "execution_computer_policy",
        "forbidden",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "memory_search",
        "memory_read",
        "thread_memory_scope",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "memory_list",
        "memory_read",
        "thread_memory_scope",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "memory_get",
        "memory_read",
        "thread_memory_scope",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "memory_remember",
        "memory_create_thread",
        "thread_memory_scope",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "memory_forget",
        "memory_forget_thread",
        "thread_memory_scope",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_create",
        "task_create",
        "root_thread_task_graph",
        "not_found",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_wait",
        "task_read",
        "root_thread_task_graph",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_result",
        "task_read|task_review",
        "immutable_task_result_candidate",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_accept",
        "task_review",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_revise",
        "task_review",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_cancel",
        "task_cancel",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_update",
        "task_schedule_manage",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_detach",
        "task_detach",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_list",
        "task_read",
        "root_thread_task_graph",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_get",
        "task_read",
        "root_thread_task",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_reschedule",
        "task_schedule_manage",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_pause",
        "task_schedule_manage",
        "root_thread_task",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "task_resume",
        "task_schedule_manage",
        "root_thread_task",
        "not_found",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "artifact_prepare",
        "artifact_create_thread",
        "thread_artifact",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "artifact_register",
        "artifact_bind_thread",
        "thread_artifact",
        "not_found",
        "mutation",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "artifact_read",
        "artifact_read",
        "thread_artifact",
        "not_found",
        "read",
        true,
    ),
    record(
        AuthorizationCatalogSurface::InternalTool,
        "read_skill",
        "skill_use",
        "execution_skill",
        "not_found",
        "read",
        true,
    ),
];

const DYNAMIC_TOOL_FAMILY_RECORDS: &[AuthorizationCatalogRecord] = &[
    record(
        AuthorizationCatalogSurface::DynamicToolFamily,
        "skill.*",
        "skill_use",
        "execution_skill_version",
        "not_found",
        "execution",
        true,
    ),
    record(
        AuthorizationCatalogSurface::DynamicToolFamily,
        "mcp.*",
        "mcp_use",
        "execution_mcp_server_version",
        "not_found",
        "execution",
        true,
    ),
];

pub(crate) fn authorization_catalog() -> Vec<AuthorizationCatalogRecord> {
    let mut catalog = Vec::with_capacity(
        NORMAL_METHOD_REGISTRY.len()
            + RESTRICTED_RPC_RECORDS.len()
            + BINARY_INGRESS_REGISTRY.len()
            + INTERNAL_TOOL_RECORDS.len()
            + DYNAMIC_TOOL_FAMILY_RECORDS.len(),
    );
    catalog.extend(NORMAL_METHOD_REGISTRY.iter().map(|entry| {
        let action = match entry.method {
            methods::THREAD_START => "thread_create_private|thread_create_workspace|thread_read",
            methods::TURN_START => "agent_turn_start|message_create",
            _ => entry.action.safe_name(),
        };
        record(
            AuthorizationCatalogSurface::Rpc,
            entry.method,
            action,
            entry.resolver.safe_name(),
            entry.disclosure.safe_name(),
            entry.audit.safe_name(),
            true,
        )
    }));
    catalog.extend_from_slice(RESTRICTED_RPC_RECORDS);
    catalog.extend(BINARY_INGRESS_REGISTRY.iter().map(|entry| {
        record(
            AuthorizationCatalogSurface::BinaryIngress,
            entry.kind.safe_name(),
            entry.action.safe_name(),
            entry.resolver.safe_name(),
            entry.disclosure.safe_name(),
            entry.audit.safe_name(),
            entry.reauthorize_each_frame,
        )
    }));
    catalog.extend_from_slice(INTERNAL_TOOL_RECORDS);
    catalog.extend_from_slice(DYNAMIC_TOOL_FAMILY_RECORDS);
    catalog
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use pioneer_tools::{ARTIFACT_DOMAIN_TOOL_NAMES, BuiltinToolDomain, builtin_tool_specs};

    use super::*;

    fn static_tool_catalog_names() -> BTreeSet<&'static str> {
        INTERNAL_TOOL_RECORDS
            .iter()
            .map(|entry| entry.entry_point)
            .collect()
    }

    fn unregistered_sensitive_tools<'a>(
        tool_names: impl IntoIterator<Item = &'a str>,
    ) -> Vec<&'a str> {
        let registered = static_tool_catalog_names();
        tool_names
            .into_iter()
            .filter(|tool_name| !registered.contains(tool_name))
            .collect()
    }

    #[test]
    fn catalog_covers_every_rpc_and_binary_ingress_exactly_once() {
        let catalog = authorization_catalog();
        let rpc = catalog
            .iter()
            .filter(|entry| {
                matches!(
                    entry.surface,
                    AuthorizationCatalogSurface::Rpc | AuthorizationCatalogSurface::RestrictedRpc
                )
            })
            .map(|entry| entry.entry_point)
            .collect::<BTreeSet<_>>();
        let expected_rpc = methods::NORMAL_METHODS
            .iter()
            .chain(methods::RESTRICTED_AUTH_METHODS)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(rpc, expected_rpc);

        let binary = catalog
            .iter()
            .filter(|entry| entry.surface == AuthorizationCatalogSurface::BinaryIngress)
            .map(|entry| entry.entry_point)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            binary,
            super::super::BinaryIngressKind::ALL
                .map(super::super::BinaryIngressKind::safe_name)
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn every_static_builtin_and_domain_tool_is_registered() {
        let mut sensitive = builtin_tool_specs()
            .into_iter()
            .map(|configured| configured.spec.name)
            .collect::<BTreeSet<_>>();
        for domain in BuiltinToolDomain::ALL {
            sensitive.extend(domain.tool_names().iter().map(|name| (*name).to_owned()));
        }
        sensitive.extend(
            ARTIFACT_DOMAIN_TOOL_NAMES
                .iter()
                .map(|name| (*name).to_owned()),
        );
        sensitive.insert("read_skill".to_owned());

        assert_eq!(
            unregistered_sensitive_tools(sensitive.iter().map(String::as_str)),
            Vec::<&str>::new()
        );
        let registered = static_tool_catalog_names();
        assert_eq!(registered.len(), INTERNAL_TOOL_RECORDS.len());
    }

    #[test]
    fn an_unregistered_sensitive_tool_fails_the_inventory_gate() {
        assert_eq!(
            unregistered_sensitive_tools(["read_file", "future_sensitive_tool"]),
            vec!["future_sensitive_tool"]
        );
    }

    #[test]
    fn catalog_is_unique_bounded_and_json_serializable() {
        let catalog = authorization_catalog();
        let mut identities = BTreeSet::new();
        for entry in &catalog {
            assert!(identities.insert((entry.surface, entry.entry_point)));
            assert!(!entry.entry_point.is_empty() && entry.entry_point.len() <= 128);
            assert!(!entry.action.is_empty());
            assert!(!entry.resource.is_empty());
            assert!(entry.reauthorize_before_side_effect);
        }
        let encoded = serde_json::to_value(&catalog).expect("catalog JSON");
        assert_eq!(encoded.as_array().map(Vec::len), Some(catalog.len()));
    }
}
