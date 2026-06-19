use std::fmt::Write as _;

use anyhow::{Result, bail};
use pioneer_gateway::{
    McpSecretGarbageCollectionReport, SecretPermissionHealthReport, SecretPermissionHealthStatus,
    SecretsStatusReport, SuperuserJwtRotationReport,
};

use crate::service;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SecretsCommand {
    Status { json: bool },
    Gc { dry_run: bool, json: bool },
    RotateJwtToken { json: bool },
}

pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<()> {
    match parse_secrets_command(args)? {
        SecretsCommand::Status { json } => {
            let report = service::secrets_status()?;
            print_status_report(&report, json)
        }
        SecretsCommand::Gc { dry_run, json } => {
            let report = service::secrets_garbage_collection(dry_run)?;
            print_garbage_collection_report(&report, json)
        }
        SecretsCommand::RotateJwtToken { json } => {
            let report = service::rotate_superuser_jwt_token()?;
            print_rotation_report(&report, json)
        }
    }
}

pub(crate) fn parse_secrets_command(
    mut args: impl Iterator<Item = String>,
) -> Result<SecretsCommand> {
    match args.next().as_deref() {
        Some("status") => parse_status_command(args),
        Some("garbage-collection") => parse_garbage_collection_command(args),
        Some("rotate-jwt-token") => parse_rotate_jwt_token_command(args),
        Some("help") | Some("--help") | Some("-h") => bail!("{}", secrets_help_text()),
        Some(command) => bail!(
            "unknown secrets command: {command}\n\n{}",
            secrets_help_text()
        ),
        None => bail!("missing secrets command\n\n{}", secrets_help_text()),
    }
}

fn parse_status_command(args: impl Iterator<Item = String>) -> Result<SecretsCommand> {
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            flag => bail!("unexpected argument for secrets status: {flag}"),
        }
    }
    Ok(SecretsCommand::Status { json })
}

fn parse_garbage_collection_command(args: impl Iterator<Item = String>) -> Result<SecretsCommand> {
    let mut dry_run = false;
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--dry-run" => dry_run = true,
            "--json" => json = true,
            flag => bail!("unexpected argument for secrets garbage-collection: {flag}"),
        }
    }
    Ok(SecretsCommand::Gc { dry_run, json })
}

fn parse_rotate_jwt_token_command(
    mut args: impl Iterator<Item = String>,
) -> Result<SecretsCommand> {
    let Some(target) = args.next() else {
        bail!("secrets rotate-jwt-token requires a target: superuser");
    };
    if target.as_str() != "superuser" {
        bail!("unsupported jwt token rotation target: {target}; expected superuser");
    }

    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            flag => bail!("unexpected argument for secrets rotate-jwt-token: {flag}"),
        }
    }

    Ok(SecretsCommand::RotateJwtToken { json })
}

fn print_status_report(report: &SecretsStatusReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        print!("{}", format_status_human(report));
    }
    Ok(())
}

fn print_garbage_collection_report(
    report: &McpSecretGarbageCollectionReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        print!("{}", format_garbage_collection_human(report));
    }
    Ok(())
}

fn print_rotation_report(report: &SuperuserJwtRotationReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        print!("{}", format_rotation_human(report));
    }
    Ok(())
}

pub(crate) fn format_status_human(report: &SecretsStatusReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Keystore: {}", report.storage_path.display());
    let _ = writeln!(output, "Encryption: {}", report.encryption.mode);
    let _ = writeln!(output, "Total entries: {}", report.total_entries);
    let _ = writeln!(
        output,
        "Provider API keys: {}",
        report.counts.provider_api_key
    );
    let _ = writeln!(output, "MCP secrets: {}", report.counts.mcp_secret);
    let _ = writeln!(
        output,
        "Superuser JWT material: {}",
        report.counts.superuser_jwt_token
    );
    let _ = writeln!(output, "User JWT tokens: {}", report.counts.user_jwt_token);
    let _ = writeln!(
        output,
        "Desktop gateway auth tokens: {}",
        report.counts.desktop_gateway_auth_token
    );
    let _ = writeln!(output, "Unknown entries: {}", report.counts.unknown);
    let _ = writeln!(output, "Permissions:");
    for permission in &report.permissions {
        write_permission_line(&mut output, permission);
    }

    if report.mcp_orphans.available {
        let _ = writeln!(
            output,
            "MCP orphan refs: {}",
            report.mcp_orphans.orphan_refs.unwrap_or(0)
        );
    } else {
        let reason = report
            .mcp_orphans
            .unavailable_reason
            .as_deref()
            .unwrap_or("unknown");
        let _ = writeln!(output, "MCP orphan refs: unavailable ({reason})");
    }

    output
}

pub(crate) fn format_garbage_collection_human(report: &McpSecretGarbageCollectionReport) -> String {
    let mut output = String::new();
    let mode = if report.dry_run { "dry-run" } else { "applied" };
    let _ = writeln!(output, "MCP secret garbage collection: {mode}");
    let _ = writeln!(output, "Active refs: {}", report.active_refs);
    let _ = writeln!(output, "Stored MCP refs: {}", report.stored_refs);
    let _ = writeln!(output, "Orphan refs: {}", report.orphan_refs);
    let _ = writeln!(output, "Deleted refs: {}", report.deleted_refs);
    let _ = writeln!(output, "Failed deletes: {}", report.failed_deletes.len());
    if !report.failed_deletes.is_empty() {
        let _ = writeln!(output, "Failed delete refs:");
        for failure in &report.failed_deletes {
            let _ = writeln!(output, "- {}: {}", failure.ref_id, failure.error);
        }
    }
    output
}

pub(crate) fn format_rotation_human(report: &SuperuserJwtRotationReport) -> String {
    let mut output = String::new();
    if report.material_existed {
        let _ = writeln!(output, "Rotated superuser JWT signing material.");
        let _ = writeln!(output, "Existing superuser bearer tokens are now invalid.");
        let _ = writeln!(
            output,
            "Run `pioneer issue-superuser-token` to issue a bearer token from the new material."
        );
    } else {
        let _ = writeln!(output, "Created superuser JWT signing material.");
        let _ = writeln!(
            output,
            "No existing superuser bearer tokens were invalidated."
        );
    }
    output
}

fn write_permission_line(output: &mut String, permission: &SecretPermissionHealthReport) {
    let actual = permission
        .actual
        .as_deref()
        .map(|actual| format!(", actual {actual}"))
        .unwrap_or_default();
    let detail = permission
        .detail
        .as_deref()
        .map(|detail| format!("; {detail}"))
        .unwrap_or_default();
    let _ = writeln!(
        output,
        "- {}: {} (expected {}{}{})",
        permission.target,
        permission_status_label(permission.status),
        permission.expected,
        actual,
        detail
    );
}

fn permission_status_label(status: SecretPermissionHealthStatus) -> &'static str {
    match status {
        SecretPermissionHealthStatus::Ok => "ok",
        SecretPermissionHealthStatus::Missing => "missing",
        SecretPermissionHealthStatus::MissingOptional => "missing optional",
        SecretPermissionHealthStatus::NotFile => "not file",
        SecretPermissionHealthStatus::NotDirectory => "not directory",
        SecretPermissionHealthStatus::TooPermissive => "too permissive",
        SecretPermissionHealthStatus::Unknown => "unknown",
        SecretPermissionHealthStatus::Error => "error",
    }
}

fn secrets_help_text() -> &'static str {
    "Usage:
  pioneer secrets status [--json]
  pioneer secrets garbage-collection [--dry-run] [--json]
  pioneer secrets rotate-jwt-token superuser [--json]"
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use pioneer_gateway::{
        KeystoreEncryptionReport, McpSecretGarbageCollectionFailure,
        McpSecretGarbageCollectionReport, McpSecretOrphanStatusReport, SecretKindCounts,
        SecretsStatusReport, SuperuserJwtRotationReport,
    };

    use super::*;

    #[test]
    fn parses_status_command() {
        assert_eq!(
            parse_secrets_command(["status"].into_iter().map(str::to_owned)).expect("parse"),
            SecretsCommand::Status { json: false }
        );
        assert_eq!(
            parse_secrets_command(["status", "--json"].into_iter().map(str::to_owned))
                .expect("parse"),
            SecretsCommand::Status { json: true }
        );
    }

    #[test]
    fn parses_garbage_collection_command_with_flexible_flag_order() {
        assert_eq!(
            parse_secrets_command(["garbage-collection"].into_iter().map(str::to_owned))
                .expect("parse"),
            SecretsCommand::Gc {
                dry_run: false,
                json: false,
            }
        );
        assert_eq!(
            parse_secrets_command(
                ["garbage-collection", "--dry-run"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .expect("parse"),
            SecretsCommand::Gc {
                dry_run: true,
                json: false,
            }
        );
        assert_eq!(
            parse_secrets_command(
                ["garbage-collection", "--json", "--dry-run"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .expect("parse"),
            SecretsCommand::Gc {
                dry_run: true,
                json: true,
            }
        );
    }

    #[test]
    fn parses_rotate_superuser_only() {
        assert_eq!(
            parse_secrets_command(
                ["rotate-jwt-token", "superuser", "--json"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .expect("parse"),
            SecretsCommand::RotateJwtToken { json: true }
        );

        assert!(
            parse_secrets_command(
                ["rotate-jwt-token", "user-123"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .is_err()
        );
        assert!(
            parse_secrets_command(["rotate-jwt-token"].into_iter().map(str::to_owned)).is_err()
        );
    }

    #[test]
    fn rejects_unknown_secrets_command_and_flags() {
        assert!(parse_secrets_command(["unknown"].into_iter().map(str::to_owned)).is_err());
        assert!(
            parse_secrets_command(["status", "--dry-run"].into_iter().map(str::to_owned)).is_err()
        );
        assert!(
            parse_secrets_command(
                ["garbage-collection", "--force"]
                    .into_iter()
                    .map(str::to_owned)
            )
            .is_err()
        );
    }

    #[test]
    fn status_human_output_contains_counts_and_no_secret_values() {
        let report = status_fixture();

        let output = format_status_human(&report);

        assert!(output.contains("Provider API keys: 1"));
        assert!(output.contains("MCP secrets: 2"));
        assert!(output.contains("MCP orphan refs: 1"));
        assert!(output.contains("runtime_home: ok"));
        assert!(!output.contains("sk-provider-secret"));
        assert!(!output.contains("mcp-secret-value"));
        assert!(!output.contains("desktop-bearer-secret"));
    }

    #[test]
    fn status_human_output_handles_unavailable_orphan_status() {
        let mut report = status_fixture();
        report.mcp_orphans.available = false;
        report.mcp_orphans.orphan_refs = None;
        report.mcp_orphans.unavailable_reason = Some("gateway_db_missing".to_owned());

        let output = format_status_human(&report);

        assert!(output.contains("MCP orphan refs: unavailable (gateway_db_missing)"));
    }

    #[test]
    fn gc_human_output_contains_counts_and_failures_without_values() {
        let report = McpSecretGarbageCollectionReport {
            dry_run: true,
            active_refs: 2,
            stored_refs: 3,
            orphan_refs: 1,
            deleted_refs: 0,
            failed_deletes: vec![McpSecretGarbageCollectionFailure {
                ref_id: "orphan_ref".to_owned(),
                error: "delete failed".to_owned(),
            }],
        };

        let output = format_garbage_collection_human(&report);

        assert!(output.contains("MCP secret garbage collection: dry-run"));
        assert!(output.contains("Orphan refs: 1"));
        assert!(output.contains("orphan_ref"));
        assert!(!output.contains("mcp-secret-value"));
    }

    #[test]
    fn rotation_human_output_never_prints_token_or_material() {
        let report = SuperuserJwtRotationReport {
            token_kind: "superuser".to_owned(),
            storage_service: "pioneer.gateway.superuser_jwt_token".to_owned(),
            storage_user: "superuser".to_owned(),
            material_existed: true,
            rotated_at_unix: 1_700_000_000,
            existing_bearer_tokens_invalidated: true,
        };

        let output = format_rotation_human(&report);

        assert!(output.contains("Rotated superuser JWT signing material."));
        assert!(output.contains("issue-superuser-token"));
        assert!(!output.contains("eyJ"));
        assert!(!output.contains("0123456789abcdef"));
    }

    #[test]
    fn json_reports_serialize_expected_keys() {
        let status_json = serde_json::to_string(&status_fixture()).expect("status json");
        assert!(status_json.contains("storage_path"));
        assert!(status_json.contains("provider_api_key"));
        assert!(!status_json.contains("sk-provider-secret"));

        let gc_json = serde_json::to_string(&McpSecretGarbageCollectionReport {
            dry_run: true,
            active_refs: 0,
            stored_refs: 1,
            orphan_refs: 1,
            deleted_refs: 0,
            failed_deletes: Vec::new(),
        })
        .expect("gc json");
        assert!(gc_json.contains("dry_run"));

        let rotation_json = serde_json::to_string(&SuperuserJwtRotationReport {
            token_kind: "superuser".to_owned(),
            storage_service: "pioneer.gateway.superuser_jwt_token".to_owned(),
            storage_user: "superuser".to_owned(),
            material_existed: true,
            rotated_at_unix: 1_700_000_000,
            existing_bearer_tokens_invalidated: true,
        })
        .expect("rotation json");
        assert!(rotation_json.contains("existing_bearer_tokens_invalidated"));
        assert!(!rotation_json.contains("eyJ"));
    }

    fn status_fixture() -> SecretsStatusReport {
        SecretsStatusReport {
            storage_path: PathBuf::from("/tmp/pioneer/keystore.db"),
            encryption: KeystoreEncryptionReport {
                enabled: false,
                mode: "disabled".to_owned(),
            },
            counts: SecretKindCounts {
                provider_api_key: 1,
                mcp_secret: 2,
                superuser_jwt_token: 1,
                user_jwt_token: 0,
                desktop_gateway_auth_token: 1,
                gateway_remote_access_secret: 0,
                unknown: 0,
            },
            total_entries: 5,
            permissions: vec![SecretPermissionHealthReport {
                path: PathBuf::from("/tmp/pioneer"),
                target: "runtime_home".to_owned(),
                status: SecretPermissionHealthStatus::Ok,
                expected: "0700".to_owned(),
                actual: Some("0700".to_owned()),
                detail: None,
            }],
            mcp_orphans: McpSecretOrphanStatusReport {
                available: true,
                gateway_db_path: PathBuf::from("/tmp/pioneer/gateway.db"),
                active_refs: Some(1),
                stored_refs: Some(2),
                orphan_refs: Some(1),
                unavailable_reason: None,
            },
        }
    }
}
