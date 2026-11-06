use crate::{
    Boundary, Diagnostic, Entry, Manifest, PortableServer, Snapshot, parse_manifest, parse_mcp,
};
use pioneer_skills::{SkillId, SkillMarkdownParseContext, SkillSourceKind, parse_skill_markdown};
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
    let bytes = snapshot.file("plugin.json").ok_or_else(|| {
        Diagnostic::new(
            "fatal_manifest",
            Boundary::Package,
            "plugin.json",
            "Manifest must resolve to a contained regular file",
        )
    })?;
    let (manifest, mut diagnostics) = parse_manifest(bytes)?;
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
                if examined > snapshot.limits().components.min(256) {
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
    match snapshot.entries().get("mcp.json") {
        None => {}
        Some(Entry::File { bytes, .. }) => {
            match parse_mcp(
                bytes,
                snapshot
                    .limits()
                    .components
                    .min(256)
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
        _ => diagnostics.push(Diagnostic::new(
            "invalid_mcp_location",
            Boundary::Mcp,
            "mcp.json",
            "MCP location must be a contained regular file",
        )),
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
    // Existing parser supplies the native contract and strict conformance. Its
    // fallback display name/description must not replace required frontmatter.
    let normalized = text.replace("\r\n", "\n");
    let Some(rest) = normalized.strip_prefix("---\n") else {
        return false;
    };
    let Some(end) = rest.find("\n---") else {
        return false;
    };
    let Ok(frontmatter) = serde_yaml::from_str::<serde_yaml::Value>(&rest[..end]) else {
        return false;
    };
    if frontmatter.get("name").and_then(serde_yaml::Value::as_str) != Some(name)
        || frontmatter
            .get("description")
            .and_then(serde_yaml::Value::as_str)
            .is_none_or(|s| s.trim().is_empty())
    {
        return false;
    }
    let Ok(skill_id) = SkillId::new("000000000000000000000") else {
        return false;
    };
    parse_skill_markdown(
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
    .is_ok_and(|s| s.conformance.agentskills_strict.compliant)
}
