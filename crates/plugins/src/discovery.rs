use crate::{Boundary, Diagnostic, Entry, Manifest, PortableServer, Snapshot, parse_mcp};
use pioneer_skills::contract::parse_skill_markdown_with_authored_fields;
use pioneer_skills::{SkillId, SkillMarkdownParseContext, SkillSourceKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComponentPlan {
    Skill {
        member_key: String,
        member_path: String,
        tree_digest: String,
    },
    Mcp {
        member_key: String,
        server: PortableServer,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedPluginPlan {
    pub spec_version: String,
    pub manifest: Manifest,
    pub tree_digest: String,
    pub components: Vec<ComponentPlan>,
    pub diagnostics: Vec<Diagnostic>,
}

pub fn load(snapshot: &Snapshot) -> Result<LoadedPluginPlan, Diagnostic> {
    let (manifest, mut diagnostics, imported) = crate::source::manifest(snapshot)?;
    let mut components = Vec::new();
    let mut examined = 0;
    match snapshot.entries().get("skills") {
        None => {}
        Some(Entry::Directory { .. }) => {
            for (key, entry) in snapshot.entries() {
                let Some(name) = key.strip_prefix("skills/").filter(|s| !s.contains('/')) else {
                    continue;
                };
                if !matches!(entry, Entry::Directory { .. }) {
                    continue;
                }
                let file = format!("{key}/SKILL.md");
                if !snapshot.entries().contains_key(&file) {
                    continue;
                }
                examined += 1;
                if examined > snapshot.limits().components {
                    return Err(limit());
                }
                match snapshot
                    .file(&file)
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                {
                    Some(text) if valid_skill(text, name) => {
                        components.push(ComponentPlan::Skill {
                            member_key: name.into(),
                            member_path: key.clone(),
                            tree_digest: snapshot.member_digest(key),
                        })
                    }
                    _ => diagnostics.push(Diagnostic::new(
                        "invalid_skill",
                        Boundary::Skill,
                        &file,
                        "Skill must conform to Agent Skills and resolve within the package",
                    )),
                }
            }
        }
        _ => diagnostics.push(Diagnostic::new(
            "invalid_skills_location",
            Boundary::Skills,
            "skills",
            "Skills location must be a contained directory",
        )),
    }
    match crate::source::mcp(snapshot, &manifest, imported) {
        Ok(None) => {}
        Ok(Some(bytes)) => {
            match parse_mcp(
                &bytes,
                snapshot
                    .limits()
                    .components
                    .saturating_sub(examined),
            ) {
                Ok(servers) => {
                    for (member_key, server) in servers {
                        match server {
                            Ok(server)
                                if crate::portable_mcp::validate_snapshot_paths(
                                    &server, snapshot,
                                ) =>
                            {
                                components.push(ComponentPlan::Mcp { member_key, server })
                            }
                            Ok(_) => diagnostics.push(Diagnostic::new(
                                "invalid_mcp_path",
                                Boundary::Server,
                                &format!(
                                    "mcp.json/mcpServers/{}",
                                    crate::diagnostic::pointer_key(&member_key)
                                ),
                                "Command or working directory fails package containment",
                            )),
                            Err(diagnostic) => diagnostics.push(diagnostic),
                        }
                    }
                }
                Err(diagnostic) if diagnostic.code == "component_limit" => return Err(diagnostic),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
        Err(diagnostic) => diagnostics.push(diagnostic),
    }
    // Access failures on ancillary paths do not change SKILL.md conformance.
    // Native folder installation still applies its existing security policy.
    for (path, entry) in snapshot.entries() {
        if matches!(entry, Entry::Denied) {
            diagnostics.push(Diagnostic::new(
                "denied_package_path",
                Boundary::HostEffect,
                path,
                "Access to this package path is denied",
            ));
        }
    }
    Ok(LoadedPluginPlan {
        spec_version: crate::SPEC_VERSION.into(),
        manifest,
        tree_digest: snapshot.tree_digest(),
        components,
        diagnostics,
    })
}
fn limit() -> Diagnostic {
    Diagnostic::new(
        "component_limit",
        Boundary::HostEffect,
        "skills",
        "Package exceeds host component limit",
    )
}
fn valid_skill(text: &str, name: &str) -> bool {
    let Ok(skill_id) = SkillId::new("000000000000000000000") else {
        return false;
    };
    parse_skill_markdown_with_authored_fields(
        text,
        SkillMarkdownParseContext {
            skill_id,
            source_kind: SkillSourceKind::User,
            source_root: String::new(),
            skill_dir: format!("skills/{name}"),
            skill_file: format!("skills/{name}/SKILL.md"),
            parent_directory_name: name.into(),
            identity_owner_override: None,
            identity_slug_override: None,
            version_hint_override: None,
            display_name_override: None,
        },
    )
    .is_ok_and(|(skill, authored)| {
        authored.name.as_deref() == Some(name)
            && authored.description.is_some()
            && skill.conformance.agentskills_strict.compliant
    })
}
