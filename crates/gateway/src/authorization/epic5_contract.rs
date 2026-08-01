//! Frozen Epic 5 ingress contract established before production dispatch.
//!
//! Later phases replace the string-level action/resource names with the
//! production protocol and authorization types while preserving this exact
//! operation set and policy ownership.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OperationContract {
    method: &'static str,
    admission: &'static str,
    action: &'static str,
    resource: &'static str,
    superuser: &'static str,
    member: &'static str,
    restricted_invitee: &'static str,
    disclosure: &'static str,
    audit: &'static str,
    rate_limit: &'static str,
    transaction_owner: &'static str,
    concurrency: &'static str,
    post_commit: &'static str,
}

const OPERATIONS: [OperationContract; 13] = [
    OperationContract {
        method: "invite/create",
        admission: "normal",
        action: "invitation_create",
        resource: "canonical_active_workspace_grant_set",
        superuser: "allow_existing_active_workspaces",
        member: "allow_current_memberships_only",
        restricted_invitee: "deny",
        disclosure: "validation_or_epic4_anti_idor",
        audit: "invitation_created",
        rate_limit: "per_principal_per_gateway_and_live_pending_cap",
        transaction_owner: "invitation_service",
        concurrency: "transaction_local_grant_reauthorization",
        post_commit: "none",
    },
    OperationContract {
        method: "invite/list",
        admission: "normal",
        action: "invitation_list",
        resource: "actor_scoped_invitation_collection",
        superuser: "allow_all_statuses_and_creators",
        member: "allow_own_effective_pending_only",
        restricted_invitee: "deny",
        disclosure: "epic4_anti_idor",
        audit: "invitation_expired_only_when_materialized",
        rate_limit: "ordinary_authenticated_read",
        transaction_owner: "invitation_service",
        concurrency: "effective_expiration_before_pagination",
        post_commit: "invitation_changed_only_on_expiration_transition",
    },
    OperationContract {
        method: "invite/revoke",
        admission: "normal",
        action: "invitation_revoke",
        resource: "actor_scoped_pending_invitation",
        superuser: "allow_any_pending",
        member: "allow_own_pending_only",
        restricted_invitee: "deny",
        disclosure: "epic4_anti_idor",
        audit: "invitation_revoked_or_invitation_expired_if_materialized",
        rate_limit: "ordinary_authenticated_mutation",
        transaction_owner: "invitation_service",
        concurrency: "conditional_pending_terminal_transition",
        post_commit: "scoped_invitation_changed",
    },
    OperationContract {
        method: "member/list",
        admission: "normal",
        action: "member_directory_list",
        resource: "gateway_member_directory",
        superuser: "allow_all_principals",
        member: "allow_self_superuser_and_shared_active_or_suspended",
        restricted_invitee: "deny",
        disclosure: "acl_before_pagination",
        audit: "none",
        rate_limit: "ordinary_authenticated_read",
        transaction_owner: "member_administration_service",
        concurrency: "current_acl_snapshot",
        post_commit: "none",
    },
    OperationContract {
        method: "member/suspend",
        admission: "normal",
        action: "member_suspend",
        resource: "ordinary_member_principal",
        superuser: "allow_non_superuser_target",
        member: "deny",
        restricted_invitee: "deny",
        disclosure: "safe_invalid_target_or_conflict",
        audit: "member_suspended_on_change",
        rate_limit: "ordinary_authenticated_management",
        transaction_owner: "member_administration_service",
        concurrency: "principal_status_and_session_family_transaction",
        post_commit: "invalidate_publish_then_terminate_target",
    },
    OperationContract {
        method: "member/restore",
        admission: "normal",
        action: "member_restore",
        resource: "suspended_ordinary_member_principal",
        superuser: "allow_non_superuser_target",
        member: "deny",
        restricted_invitee: "deny",
        disclosure: "safe_invalid_target_or_conflict",
        audit: "member_restored_on_change",
        rate_limit: "ordinary_authenticated_management",
        transaction_owner: "member_administration_service",
        concurrency: "expected_status_transition",
        post_commit: "scoped_member_changed_without_credential_revival",
    },
    OperationContract {
        method: "member/remove",
        admission: "normal",
        action: "member_remove",
        resource: "ordinary_member_principal",
        superuser: "allow_non_superuser_target",
        member: "deny",
        restricted_invitee: "deny",
        disclosure: "safe_invalid_target_or_conflict",
        audit: "member_removed_on_change",
        rate_limit: "ordinary_authenticated_management",
        transaction_owner: "member_administration_service",
        concurrency: "principal_sessions_memberships_and_pending_invites_transaction",
        post_commit: "invalidate_publish_then_terminate_target",
    },
    OperationContract {
        method: "member/device/create",
        admission: "normal",
        action: "member_recovery_device_create",
        resource: "active_ordinary_member_principal",
        superuser: "allow_non_superuser_target",
        member: "deny_use_own_device_flow_instead",
        restricted_invitee: "deny",
        disclosure: "safe_invalid_target_or_conflict",
        audit: "member_recovery_device_created_on_change",
        rate_limit: "per_target_and_single_live_pending_activation",
        transaction_owner: "member_administration_service_via_auth_primitive",
        concurrency: "single_live_pending_recovery_activation",
        post_commit: "scoped_member_changed",
    },
    OperationContract {
        method: "workspace/member/list",
        admission: "normal",
        action: "workspace_member_list",
        resource: "explicit_workspace_memberships",
        superuser: "allow_any_workspace",
        member: "allow_current_workspace_only",
        restricted_invitee: "deny",
        disclosure: "epic4_anti_idor",
        audit: "none",
        rate_limit: "ordinary_authenticated_read",
        transaction_owner: "member_administration_service",
        concurrency: "current_workspace_acl_snapshot",
        post_commit: "none",
    },
    OperationContract {
        method: "workspace/member/add",
        admission: "normal",
        action: "workspace_member_add",
        resource: "workspace_and_stable_target_principal",
        superuser: "allow_any_active_workspace",
        member: "allow_current_workspace_only",
        restricted_invitee: "deny",
        disclosure: "generic_target_unavailable",
        audit: "workspace_member_added_on_change",
        rate_limit: "per_actor_direct_add",
        transaction_owner: "member_administration_service",
        concurrency: "membership_identity_serialized_with_remove",
        post_commit: "access_changed_and_scoped_members_changed",
    },
    OperationContract {
        method: "workspace/member/remove",
        admission: "normal",
        action: "workspace_member_remove",
        resource: "workspace_and_stable_target_principal",
        superuser: "allow_non_superuser_target",
        member: "deny",
        restricted_invitee: "deny",
        disclosure: "epic4_anti_idor",
        audit: "workspace_member_removed_on_change",
        rate_limit: "ordinary_authenticated_management",
        transaction_owner: "member_administration_service",
        concurrency: "membership_identity_serialized_with_add",
        post_commit: "evict_access_and_scoped_members_changed",
    },
    OperationContract {
        method: "invite/preview",
        admission: "restricted_invitation",
        action: "restricted_invitation_preview",
        resource: "pending_invitation_by_hmac_and_persisted_grants",
        superuser: "deny_on_normal_transport",
        member: "deny_on_normal_transport",
        restricted_invitee: "allow_exact_invitation_credential_only",
        disclosure: "invitation_unavailable",
        audit: "invitation_expired_or_revoked_only_if_materialized",
        rate_limit: "per_connection_source_and_token_fingerprint",
        transaction_owner: "invitation_service",
        concurrency: "pending_terminal_transition_competes_with_accept_or_revoke",
        post_commit: "scoped_invitation_changed_only_on_terminal_transition",
    },
    OperationContract {
        method: "invite/accept",
        admission: "restricted_invitation",
        action: "restricted_invitation_accept",
        resource: "pending_invitation_and_exact_persisted_grants",
        superuser: "deny_on_normal_transport",
        member: "deny_on_normal_transport",
        restricted_invitee: "allow_exact_invitation_credential_only",
        disclosure: "invitation_unavailable_or_bounded_corrective_error_after_admission",
        audit: "invitation_accepted_or_terminal_grant_invalidity",
        rate_limit: "per_connection_source_and_token_fingerprint",
        transaction_owner: "invitation_service_via_transaction_scoped_auth_primitive",
        concurrency: "one_pending_accept_winner_with_inviter_reauthorization",
        post_commit: "invalidate_publish_membership_then_expose_session_grant",
    },
];

const EXPECTED_NORMAL_METHODS: [&str; 11] = [
    "invite/create",
    "invite/list",
    "invite/revoke",
    "member/list",
    "member/suspend",
    "member/restore",
    "member/remove",
    "member/device/create",
    "workspace/member/list",
    "workspace/member/add",
    "workspace/member/remove",
];

const EXPECTED_RESTRICTED_METHODS: [&str; 2] = ["invite/preview", "invite/accept"];

const FIXED_LIMITS: [&str; 11] = [
    "workspace_grants_1_to_64",
    "display_name_1_to_128_scalars_and_512_utf8_bytes",
    "nickname_2_to_32_ascii",
    "avatar_decoded_max_256_kib",
    "avatar_dimensions_max_1024_by_1024",
    "avatar_media_png_jpeg_webp",
    "audit_metadata_max_16_kib",
    "invitation_token_256_bits_with_pinv1_prefix",
    "invitation_ttl_exactly_7_days",
    "uri_uses_activation_parser_endpoint_limits",
    "restricted_transport_uses_existing_bounded_frame_limits",
];

const SAFE_ERRORS: [&str; 10] = [
    "invitation_unavailable",
    "invalid_profile",
    "nickname_unavailable",
    "invalid_installation",
    "avatar_invalid",
    "insecure_transport_is_warning_not_rejection",
    "epic4_forbidden_not_found_mapping",
    "stale_expected_state_conflict",
    "invalid_grant_set_invalid_params",
    "internal_error_with_correlation_id_only",
];

const REQUIRED_RACES: [&str; 7] = [
    "accept_vs_accept",
    "accept_vs_revoke_or_expire",
    "accept_vs_inviter_access_loss",
    "direct_add_vs_membership_remove",
    "suspend_or_remove_vs_refresh",
    "remove_vs_invitation_accept",
    "commit_before_invalidate_publish_terminate",
];

/// Each frozen ingress has one implementation owner and one in-process test
/// seam. Keeping this beside the policy matrix prevents a later phase from
/// silently dropping a method or deferring it outside Epic 5.
const IMPLEMENTATION_OWNERS: [(&str, &str, &str); 13] = [
    ("invite/create", "P02-WP03", "invitation_service_tests"),
    ("invite/list", "P02-WP04", "invitation_service_tests"),
    ("invite/revoke", "P02-WP04", "invitation_service_tests"),
    ("member/list", "P04-WP01", "member_directory_tests"),
    ("member/suspend", "P05-WP01", "member_lifecycle_tests"),
    ("member/restore", "P05-WP02", "member_lifecycle_tests"),
    ("member/remove", "P05-WP03", "member_lifecycle_tests"),
    (
        "member/device/create",
        "P05-WP02",
        "member_recovery_device_tests",
    ),
    (
        "workspace/member/list",
        "P04-WP02",
        "workspace_member_directory_tests",
    ),
    (
        "workspace/member/add",
        "P04-WP02",
        "workspace_membership_mutation_tests",
    ),
    (
        "workspace/member/remove",
        "P04-WP03",
        "workspace_membership_mutation_tests",
    ),
    ("invite/preview", "P03-WP02", "restricted_invitation_tests"),
    ("invite/accept", "P03-WP05", "invitation_acceptance_tests"),
];

#[test]
fn epic5_operation_contract_is_exact_complete_and_unique() {
    let mut normal = OPERATIONS
        .iter()
        .filter(|operation| operation.admission == "normal")
        .map(|operation| operation.method)
        .collect::<Vec<_>>();
    let mut restricted = OPERATIONS
        .iter()
        .filter(|operation| operation.admission == "restricted_invitation")
        .map(|operation| operation.method)
        .collect::<Vec<_>>();
    normal.sort_unstable();
    restricted.sort_unstable();

    let mut expected_normal = EXPECTED_NORMAL_METHODS.to_vec();
    let mut expected_restricted = EXPECTED_RESTRICTED_METHODS.to_vec();
    expected_normal.sort_unstable();
    expected_restricted.sort_unstable();

    assert_eq!(normal, expected_normal);
    assert_eq!(restricted, expected_restricted);
    let unique = OPERATIONS
        .iter()
        .map(|operation| operation.method)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), OPERATIONS.len());
}

#[test]
fn every_epic5_operation_has_a_complete_fail_closed_classification() {
    for operation in OPERATIONS {
        for value in [
            operation.method,
            operation.admission,
            operation.action,
            operation.resource,
            operation.superuser,
            operation.member,
            operation.restricted_invitee,
            operation.disclosure,
            operation.audit,
            operation.rate_limit,
            operation.transaction_owner,
            operation.concurrency,
            operation.post_commit,
        ] {
            assert!(
                !value.is_empty(),
                "incomplete contract for {}",
                operation.method
            );
        }
        if operation.admission == "restricted_invitation" {
            assert_eq!(operation.superuser, "deny_on_normal_transport");
            assert_eq!(operation.member, "deny_on_normal_transport");
            assert_eq!(
                operation.restricted_invitee,
                "allow_exact_invitation_credential_only"
            );
        } else {
            assert_eq!(operation.restricted_invitee, "deny");
        }
    }
}

#[test]
fn epic5_limits_errors_and_races_are_frozen_without_client_authority() {
    assert_eq!(FIXED_LIMITS.len(), 11);
    assert_eq!(SAFE_ERRORS.len(), 10);
    assert_eq!(REQUIRED_RACES.len(), 7);
    assert!(SAFE_ERRORS.contains(&"invitation_unavailable"));
    assert!(REQUIRED_RACES.contains(&"accept_vs_accept"));
    assert!(REQUIRED_RACES.contains(&"commit_before_invalidate_publish_terminate"));
    assert!(OPERATIONS.iter().all(|operation| {
        !operation.resource.contains("client_grant") && !operation.action.contains("member_manage")
    }));
}

#[test]
fn every_epic5_operation_has_one_later_wp_and_hermetic_test_seam() {
    let operation_methods = OPERATIONS
        .iter()
        .map(|operation| operation.method)
        .collect::<std::collections::BTreeSet<_>>();
    let owned_methods = IMPLEMENTATION_OWNERS
        .iter()
        .map(|(method, _, _)| *method)
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(owned_methods, operation_methods);
    assert_eq!(IMPLEMENTATION_OWNERS.len(), operation_methods.len());
    for (method, wp, seam) in IMPLEMENTATION_OWNERS {
        assert!(!method.is_empty());
        assert!(wp.starts_with('P') && wp.contains("-WP"));
        assert!(seam.ends_with("_tests"));
    }
}
