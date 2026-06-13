use pioneer_artifacts::PreparedArtifactOutput;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArtifactFinalizationDiagnosticCode {
    PreparedOutputUnregistered,
    PrivateOutputPathMentioned,
}

impl ArtifactFinalizationDiagnosticCode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::PreparedOutputUnregistered => "artifact_output.prepared_unregistered",
            Self::PrivateOutputPathMentioned => "artifact_output.private_path_mentioned",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArtifactFinalizationDiagnostic {
    pub code: ArtifactFinalizationDiagnosticCode,
    pub message: String,
    pub path: Option<String>,
}

pub(super) fn artifact_finalization_retry_instruction(
    diagnostics: &[ArtifactFinalizationDiagnostic],
    retry_already_used: bool,
) -> Option<String> {
    if retry_already_used || diagnostics.is_empty() {
        return None;
    }

    let mut paths = diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.code == ArtifactFinalizationDiagnosticCode::PreparedOutputUnregistered
        })
        .filter_map(|diagnostic| diagnostic.path.as_deref())
        .filter(|path| !path.trim().is_empty())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.dedup();

    let mut instruction = String::from(
        "Artifact finalization correction required. Your previous final answer cannot be accepted because an artifact output prepared through `artifact_prepare` was written or referenced but not registered. Call `artifact_register` for each prepared artifact output before giving the final answer. Do not mention `$PIONEER_ARTIFACT_OUTPUT_DIR` or private gateway paths in the final answer.",
    );
    if !paths.is_empty() {
        instruction.push_str("\n\nPrepared output paths to register:");
        for path in paths {
            instruction.push_str("\n- ");
            instruction.push_str(path);
        }
    }
    instruction.push_str(
        "\n\nAfter the `artifact_register` tool call succeeds, answer briefly and refer to the registered artifact, not to a private path.",
    );
    Some(instruction)
}

pub(super) fn artifact_finalization_terminal_error(
    diagnostics: &[ArtifactFinalizationDiagnostic],
) -> String {
    let mut message = String::from(
        "artifact finalization repair still required after one artifact_register retry",
    );
    if diagnostics.is_empty() {
        return message;
    }

    message.push_str(": ");
    let details = diagnostics
        .iter()
        .map(|diagnostic| {
            if let Some(path) = diagnostic.path.as_deref()
                && !path.is_empty()
            {
                format!("{} ({path})", diagnostic.code.as_str())
            } else {
                diagnostic.code.as_str().to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    message.push_str(details.as_str());
    message
}

pub(super) fn diagnose_artifact_finalization(
    prepared_outputs: &[PreparedArtifactOutput],
    output_dir: Option<&str>,
    final_text: Option<&str>,
) -> Vec<ArtifactFinalizationDiagnostic> {
    let mut diagnostics = Vec::new();

    for output in prepared_outputs
        .iter()
        .filter(|output| !output.is_registered())
        .filter(|output| prepared_output_is_materialized(output))
    {
        let path = output.output_path.display().to_string();
        diagnostics.push(ArtifactFinalizationDiagnostic {
            code: ArtifactFinalizationDiagnosticCode::PreparedOutputUnregistered,
            message: format!(
                "prepared artifact output `{path}` was not registered; call artifact_register after writing artifact_prepare outputs"
            ),
            path: Some(path),
        });
    }

    let Some(final_text) = final_text else {
        return diagnostics;
    };

    if final_text.contains("$PIONEER_ARTIFACT_OUTPUT_DIR") {
        diagnostics.push(ArtifactFinalizationDiagnostic {
            code: ArtifactFinalizationDiagnosticCode::PrivateOutputPathMentioned,
            message: "final assistant text mentioned $PIONEER_ARTIFACT_OUTPUT_DIR; call artifact_register and refer to the registered artifact instead".to_owned(),
            path: Some("$PIONEER_ARTIFACT_OUTPUT_DIR".to_owned()),
        });
    }

    for private_path in private_artifact_output_paths(prepared_outputs, output_dir) {
        if !private_path.is_empty() && final_text.contains(private_path.as_str()) {
            diagnostics.push(ArtifactFinalizationDiagnostic {
                code: ArtifactFinalizationDiagnosticCode::PrivateOutputPathMentioned,
                message: format!(
                    "final assistant text exposed private artifact output path `{private_path}`; call artifact_register and refer to the registered artifact instead"
                ),
                path: Some(private_path),
            });
        }
    }

    diagnostics
}

fn prepared_output_is_materialized(output: &PreparedArtifactOutput) -> bool {
    match std::fs::symlink_metadata(output.output_path.as_path()) {
        Ok(metadata) => metadata.is_file() || metadata.file_type().is_symlink(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

fn private_artifact_output_paths(
    prepared_outputs: &[PreparedArtifactOutput],
    output_dir: Option<&str>,
) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(output_dir) = output_dir.map(str::trim)
        && !output_dir.is_empty()
    {
        paths.push(output_dir.to_owned());
    }
    for output in prepared_outputs {
        paths.push(output.output_path.display().to_string());
        paths.push(output.output_dir.display().to_string());
    }
    paths.sort();
    paths.dedup();
    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_artifacts::PreparedArtifactOutputStatus;
    use pioneer_protocol::ArtifactPrepareKind;
    use std::path::PathBuf;

    fn prepared(path: &str, registered: bool) -> PreparedArtifactOutput {
        PreparedArtifactOutput {
            tool_call_id: "tool-call-1".to_owned(),
            output_path: PathBuf::from(path),
            output_dir: PathBuf::from("/tmp/pioneer-output"),
            display_name: "report.txt".to_owned(),
            kind: ArtifactPrepareKind::Document,
            mime_type: None,
            description: None,
            expires_at: "2026-05-17T00:00:00Z".to_owned(),
            status: if registered {
                PreparedArtifactOutputStatus::Registered {
                    artifact_id: "artifact-1".to_owned(),
                    version_id: "version-1".to_owned(),
                }
            } else {
                PreparedArtifactOutputStatus::Reserved
            },
        }
    }

    fn materialized_prepared(registered: bool) -> (tempfile::TempDir, PreparedArtifactOutput) {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("report.txt");
        std::fs::write(path.as_path(), b"artifact").expect("write prepared output");
        let mut output = prepared(path.to_str().expect("utf8 temp path"), registered);
        output.output_dir = temp.path().to_path_buf();
        (temp, output)
    }

    #[test]
    fn artifact_diagnostic_reports_prepared_but_unregistered_output() {
        let (_temp, output) = materialized_prepared(false);
        let diagnostics = diagnose_artifact_finalization(&[output], None, Some("Done."));

        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ArtifactFinalizationDiagnosticCode::PreparedOutputUnregistered
                && diagnostic.message.contains("artifact_register")
        }));
    }

    #[test]
    fn artifact_diagnostic_ignores_registered_prepared_output() {
        let (_temp, output) = materialized_prepared(true);
        let diagnostics =
            diagnose_artifact_finalization(&[output], None, Some("Registered artifact is ready."));

        assert!(diagnostics.is_empty());
    }

    #[test]
    fn artifact_diagnostic_ignores_unused_unregistered_prepare_without_file() {
        let diagnostics = diagnose_artifact_finalization(
            &[prepared("/tmp/pioneer-output/missing-report.txt", false)],
            Some("/tmp/pioneer-output"),
            Some("Done."),
        );

        assert!(diagnostics.is_empty());
    }

    #[test]
    fn artifact_diagnostic_reports_private_output_dir_in_final_text() {
        let diagnostics = diagnose_artifact_finalization(
            &[],
            Some("/tmp/pioneer-output"),
            Some("Saved to /tmp/pioneer-output/report.txt"),
        );

        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ArtifactFinalizationDiagnosticCode::PrivateOutputPathMentioned
                && diagnostic.message.contains("artifact_register")
        }));
    }

    #[test]
    fn artifact_diagnostic_reports_output_dir_env_var_in_final_text() {
        let diagnostics = diagnose_artifact_finalization(
            &[],
            None,
            Some("Saved under $PIONEER_ARTIFACT_OUTPUT_DIR/report.txt"),
        );

        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ArtifactFinalizationDiagnosticCode::PrivateOutputPathMentioned
        }));
    }

    #[test]
    fn artifact_diagnostic_does_not_retry_or_flag_normal_text() {
        let diagnostics =
            diagnose_artifact_finalization(&[], None, Some("The registered artifact is ready."));

        assert!(diagnostics.is_empty());
        assert!(artifact_finalization_retry_instruction(&diagnostics, false).is_none());
    }

    #[test]
    fn artifact_retry_instruction_names_artifact_register_and_safe_path() {
        let (_temp, output) = materialized_prepared(false);
        let output_path = output.output_path.display().to_string();
        let diagnostics = diagnose_artifact_finalization(&[output], None, Some("Done."));

        let instruction = artifact_finalization_retry_instruction(&diagnostics, false)
            .expect("unregistered prepared output should trigger one retry");

        assert!(instruction.contains("artifact_register"));
        assert!(instruction.contains(output_path.as_str()));
    }

    #[test]
    fn artifact_retry_instruction_is_suppressed_after_retry_used() {
        let (_temp, output) = materialized_prepared(false);
        let diagnostics = diagnose_artifact_finalization(&[output], None, Some("Done."));

        assert!(artifact_finalization_retry_instruction(&diagnostics, true).is_none());
    }

    #[test]
    fn artifact_retry_successful_registration_clears_diagnostic() {
        let (_temp, output) = materialized_prepared(true);
        let diagnostics =
            diagnose_artifact_finalization(&[output], None, Some("Registered artifact is ready."));

        assert!(diagnostics.is_empty());
        assert!(artifact_finalization_retry_instruction(&diagnostics, false).is_none());
    }

    #[test]
    fn artifact_retry_terminal_error_reports_failure_after_one_attempt() {
        let (_temp, output) = materialized_prepared(false);
        let diagnostics = diagnose_artifact_finalization(&[output], None, Some("Done."));

        let error = artifact_finalization_terminal_error(&diagnostics);

        assert!(error.contains("after one artifact_register retry"));
        assert!(error.contains("artifact_output.prepared_unregistered"));
    }
}
