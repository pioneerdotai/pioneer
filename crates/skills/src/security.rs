use crate::contract::SkillTrustLevel;
use crate::runtime::SkillRuntimeToolKind;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityDecision {
    Allow,
    Warn,
    Block,
}

impl Default for SecurityDecision {
    fn default() -> Self {
        Self::Allow
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityFinding {
    pub rule_id: String,
    pub severity: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SecurityScanReport {
    pub decision: SecurityDecision,
    #[serde(default)]
    pub findings: Vec<SecurityFinding>,
}

impl SecurityScanReport {
    pub fn has_blocking_findings(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.severity.as_str() == "block")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillSecurityPolicy {
    pub allow_untrusted_install: bool,
    pub min_trust_for_shell_tools: SkillTrustLevel,
    pub min_trust_for_http_tools: SkillTrustLevel,
    pub min_trust_for_function_proxy_tools: SkillTrustLevel,
    pub max_install_archive_bytes: usize,
    pub max_install_file_bytes: usize,
}

impl Default for SkillSecurityPolicy {
    fn default() -> Self {
        Self {
            allow_untrusted_install: false,
            min_trust_for_shell_tools: SkillTrustLevel::Verified,
            min_trust_for_http_tools: SkillTrustLevel::Community,
            min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
            max_install_archive_bytes: 10 * 1024 * 1024,
            max_install_file_bytes: 1024 * 1024,
        }
    }
}

pub fn scan_skill_directory(
    source_root: &Path,
    skill_dir: &Path,
    max_file_bytes: usize,
) -> SecurityScanReport {
    let mut findings = Vec::new();

    let Some(root_canonical) = canonicalize_optional(source_root) else {
        findings.push(block_finding(
            "path.root_missing",
            "source root cannot be canonicalized",
            Some(source_root),
        ));
        return SecurityScanReport {
            decision: SecurityDecision::Block,
            findings,
        };
    };

    let Some(skill_canonical) = canonicalize_optional(skill_dir) else {
        findings.push(block_finding(
            "path.skill_missing",
            "skill directory cannot be canonicalized",
            Some(skill_dir),
        ));
        return SecurityScanReport {
            decision: SecurityDecision::Block,
            findings,
        };
    };

    if !is_within(root_canonical.as_path(), skill_canonical.as_path()) {
        findings.push(block_finding(
            "path.containment",
            "skill directory is outside configured source root",
            Some(skill_dir),
        ));
        return SecurityScanReport {
            decision: SecurityDecision::Block,
            findings,
        };
    }

    let mut queue = VecDeque::new();
    queue.push_back(skill_canonical);

    while let Some(current) = queue.pop_front() {
        let Ok(entries) = fs::read_dir(current.as_path()) else {
            continue;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = fs::symlink_metadata(path.as_path()) else {
                continue;
            };

            if metadata.file_type().is_symlink() {
                match fs::canonicalize(path.as_path()) {
                    Ok(target_canonical)
                        if is_within(root_canonical.as_path(), target_canonical.as_path()) =>
                    {
                        findings.push(warn_finding(
                            "path.symlink_present",
                            "skill contains symlink; kept under root containment",
                            Some(path.as_path()),
                        ));
                    }
                    Ok(_) => findings.push(block_finding(
                        "path.symlink_escape",
                        "symlink resolves outside configured source root",
                        Some(path.as_path()),
                    )),
                    Err(_) => findings.push(block_finding(
                        "path.symlink_broken",
                        "symlink target cannot be resolved",
                        Some(path.as_path()),
                    )),
                }
                continue;
            }

            if metadata.is_dir() {
                queue.push_back(path);
                continue;
            }

            if metadata.is_file() {
                if metadata.len() > max_file_bytes as u64 {
                    findings.push(block_finding(
                        "file.size_limit",
                        format!("file exceeds max size of {max_file_bytes} bytes"),
                        Some(path.as_path()),
                    ));
                }

                if has_suspicious_executable_suffix(path.as_path()) {
                    findings.push(warn_finding(
                        "file.suspicious_executable",
                        "skill contains executable/script-like file extension",
                        Some(path.as_path()),
                    ));
                }
            }
        }
    }

    SecurityScanReport {
        decision: summarize_decision(findings.as_slice()),
        findings,
    }
}

pub fn scan_archive_entries(entries: &[String]) -> SecurityScanReport {
    let mut findings = Vec::new();

    for entry in entries {
        let normalized = entry.trim();
        if normalized.is_empty() {
            continue;
        }

        let path = Path::new(normalized);
        if path.is_absolute() {
            findings.push(block_finding(
                "archive.absolute_path",
                "archive entry uses absolute path",
                Some(path),
            ));
            continue;
        }

        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            findings.push(block_finding(
                "archive.traversal",
                "archive entry contains parent traversal (`..`)",
                Some(path),
            ));
        }
    }

    SecurityScanReport {
        decision: summarize_decision(findings.as_slice()),
        findings,
    }
}

pub fn ensure_install_path_contained(
    install_root: &Path,
    install_path: &Path,
) -> SecurityScanReport {
    let mut findings = Vec::new();
    let root = canonicalize_optional(install_root);
    let target = canonicalize_optional(install_path).or_else(|| {
        install_path
            .parent()
            .and_then(canonicalize_optional)
            .map(|base| base.join(install_path.file_name().unwrap_or_default()))
    });

    match (root, target) {
        (Some(root), Some(target)) => {
            if !is_within(root.as_path(), target.as_path()) {
                findings.push(block_finding(
                    "install.containment",
                    "install path is outside managed install root",
                    Some(install_path),
                ));
            }
        }
        _ => findings.push(block_finding(
            "install.path_unresolved",
            "install path or install root cannot be canonicalized",
            Some(install_path),
        )),
    }

    SecurityScanReport {
        decision: summarize_decision(findings.as_slice()),
        findings,
    }
}

pub fn trust_satisfies_minimum(actual: SkillTrustLevel, required: SkillTrustLevel) -> bool {
    trust_rank(actual) >= trust_rank(required)
}

pub fn minimum_trust_for_tool_kind(
    kind: &SkillRuntimeToolKind,
    policy: &SkillSecurityPolicy,
) -> SkillTrustLevel {
    match kind {
        SkillRuntimeToolKind::Shell => policy.min_trust_for_shell_tools.clone(),
        SkillRuntimeToolKind::Http => policy.min_trust_for_http_tools.clone(),
        SkillRuntimeToolKind::FunctionProxy => policy.min_trust_for_function_proxy_tools.clone(),
    }
}

fn trust_rank(level: SkillTrustLevel) -> u8 {
    match level {
        SkillTrustLevel::Untrusted => 0,
        SkillTrustLevel::Community => 1,
        SkillTrustLevel::Verified => 2,
        SkillTrustLevel::Internal => 3,
    }
}

fn summarize_decision(findings: &[SecurityFinding]) -> SecurityDecision {
    if findings.iter().any(|finding| finding.severity == "block") {
        return SecurityDecision::Block;
    }
    if findings.iter().any(|finding| finding.severity == "warn") {
        return SecurityDecision::Warn;
    }
    SecurityDecision::Allow
}

fn block_finding(
    rule_id: impl Into<String>,
    message: impl Into<String>,
    path: Option<&Path>,
) -> SecurityFinding {
    SecurityFinding {
        rule_id: rule_id.into(),
        severity: "block".to_owned(),
        message: message.into(),
        path: path.map(|value| value.display().to_string()),
    }
}

fn warn_finding(
    rule_id: impl Into<String>,
    message: impl Into<String>,
    path: Option<&Path>,
) -> SecurityFinding {
    SecurityFinding {
        rule_id: rule_id.into(),
        severity: "warn".to_owned(),
        message: message.into(),
        path: path.map(|value| value.display().to_string()),
    }
}

fn canonicalize_optional(path: &Path) -> Option<PathBuf> {
    fs::canonicalize(path).ok()
}

fn is_within(root: &Path, candidate: &Path) -> bool {
    let root_components = root.components().collect::<Vec<_>>();
    let candidate_components = candidate.components().collect::<Vec<_>>();
    if candidate_components.len() < root_components.len() {
        return false;
    }
    root_components
        .iter()
        .zip(candidate_components.iter())
        .all(|(left, right)| left == right)
}

fn has_suspicious_executable_suffix(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|value| value.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "sh" | "bash" | "zsh" | "fish" | "ps1" | "cmd" | "bat" | "exe"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        SecurityDecision, SkillSecurityPolicy, minimum_trust_for_tool_kind, scan_archive_entries,
        scan_skill_directory, trust_satisfies_minimum,
    };
    use crate::contract::SkillTrustLevel;
    use crate::runtime::SkillRuntimeToolKind;
    use std::fs;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix timestamp")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skills-security-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn archive_traversal_is_blocked() {
        let report = scan_archive_entries(&[
            "skill/SKILL.md".to_owned(),
            "../escape/SKILL.md".to_owned(),
            "/absolute/path".to_owned(),
        ]);
        assert_eq!(report.decision, SecurityDecision::Block);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.rule_id == "archive.traversal")
        );
    }

    #[test]
    fn suspicious_script_is_warned() {
        let root = temp_dir("warn");
        let skill = root.join("my-skill");
        fs::create_dir_all(&skill).expect("create skill dir");
        fs::write(skill.join("SKILL.md"), "body").expect("write skill file");
        fs::write(skill.join("run.sh"), "#!/bin/sh\necho hi").expect("write script");

        let report = scan_skill_directory(root.as_path(), skill.as_path(), 1024 * 1024);
        assert_eq!(report.decision, SecurityDecision::Warn);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.rule_id == "file.suspicious_executable")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn symlink_escape_is_blocked() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let root = temp_dir("symlink");
            let outside = temp_dir("outside");
            let outside_file = outside.join("outside.txt");
            fs::write(&outside_file, "outside").expect("write outside");

            let skill = root.join("my-skill");
            fs::create_dir_all(&skill).expect("create skill dir");
            fs::write(skill.join("SKILL.md"), "body").expect("write skill file");
            symlink(outside_file.as_path(), skill.join("leak")).expect("create symlink");

            let report = scan_skill_directory(root.as_path(), skill.as_path(), 1024 * 1024);
            assert_eq!(report.decision, SecurityDecision::Block);
            assert!(
                report
                    .findings
                    .iter()
                    .any(|finding| finding.rule_id == "path.symlink_escape")
            );

            let _ = fs::remove_dir_all(root);
            let _ = fs::remove_dir_all(outside);
        }
    }

    #[test]
    fn trust_gate_mapping_is_stable() {
        let policy = SkillSecurityPolicy::default();
        assert_eq!(
            minimum_trust_for_tool_kind(&SkillRuntimeToolKind::Shell, &policy),
            SkillTrustLevel::Verified
        );
        assert!(trust_satisfies_minimum(
            SkillTrustLevel::Internal,
            SkillTrustLevel::Verified
        ));
        assert!(!trust_satisfies_minimum(
            SkillTrustLevel::Community,
            SkillTrustLevel::Verified
        ));
    }
}
