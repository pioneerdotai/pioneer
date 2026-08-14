use crate::compile::{
    CompileSkillInput, SkillAvailability, SkillDefinition, SkillUnavailableReason,
    compile_skill_definition,
};
use crate::contract::{
    SkillCatalogSnapshot, SkillDependencies, SkillSourceKind, SkillTrustLevel,
    default_skill_conformance, fingerprint_for_content, parse_skill_from_file,
};
use anyhow::{Result, bail};
use pioneer_protocol::SkillId;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SkillCatalogInstallation {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub version: Option<String>,
    pub source_kind: SkillSourceKind,
    pub source_ref: String,
    pub install_path: PathBuf,
    pub trust_level: SkillTrustLevel,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct BundledSkillCatalogEntry {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub source_root: PathBuf,
    pub install_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SkillCatalogLoadParams {
    pub installations: Vec<SkillCatalogInstallation>,
    pub bundled: Vec<BundledSkillCatalogEntry>,
    pub max_file_bytes: usize,
}

fn now_unix_seconds() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn now_version() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn installation_is_import_pending(installation: &SkillCatalogInstallation) -> bool {
    installation
        .source_ref
        .strip_prefix("import-path:")
        .is_some_and(|source_path| installation.install_path == Path::new(source_path))
}

fn source_root_for_installation(install_path: &Path) -> PathBuf {
    install_path
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| install_path.to_path_buf())
}

fn bundled_skill_fingerprint(entry: &BundledSkillCatalogEntry) -> String {
    let skill_file = entry.install_path.join("SKILL.md");
    match fs::read_to_string(skill_file) {
        Ok(content) => fingerprint_for_content(content.replace("\r\n", "\n").as_str()),
        Err(_) => fingerprint_for_content(
            format!(
                "pioneer-bundled-skill-unavailable-v1\n{}\n{}",
                entry.skill_id, entry.slug
            )
            .as_str(),
        ),
    }
}

struct CatalogMetadata<'a> {
    skill_id: &'a SkillId,
    owner: &'a Option<String>,
    slug: &'a str,
    version: &'a Option<String>,
    source_kind: SkillSourceKind,
    source_root: &'a Path,
    install_path: &'a Path,
    trust_level: SkillTrustLevel,
    fingerprint: &'a str,
}

fn unavailable_definition(
    metadata: &CatalogMetadata<'_>,
    reason: SkillUnavailableReason,
) -> SkillDefinition {
    let mut definition = compile_skill_definition(CompileSkillInput {
        skill_id: metadata.skill_id.clone(),
        owner: metadata.owner.clone(),
        slug: metadata.slug.to_owned(),
        name: metadata.slug.to_owned(),
        display_name: metadata.slug.to_owned(),
        description: "Skill package is unavailable.".to_owned(),
        body: String::new(),
        source_kind: metadata.source_kind,
        source_root: metadata.source_root.display().to_string(),
        skill_dir: metadata.install_path.display().to_string(),
        skill_file: metadata.install_path.join("SKILL.md").display().to_string(),
        version_hint: metadata.version.clone(),
        fingerprint: metadata.fingerprint.to_owned(),
        user_invocable: false,
        disable_model_invocation: true,
        paths: Vec::new(),
        allowed_tools: Vec::new(),
        runtime_tools: Vec::new(),
        trust_level: metadata.trust_level,
        dependencies: SkillDependencies::default(),
        license: None,
        compatibility: None,
        metadata_raw: serde_json::json!({}),
        conformance: default_skill_conformance(),
    });
    definition.availability = SkillAvailability::Unavailable { reason };
    definition
}

fn parse_catalog_entry(metadata: &CatalogMetadata<'_>, max_file_bytes: usize) -> SkillDefinition {
    let skill_file = metadata.install_path.join("SKILL.md");
    if !metadata.install_path.is_dir() || !skill_file.is_file() {
        return unavailable_definition(metadata, SkillUnavailableReason::MissingPackage);
    }

    let mut definition = match parse_skill_from_file(
        metadata.skill_id.clone(),
        skill_file.as_path(),
        metadata.source_kind,
        metadata.source_root,
        max_file_bytes,
    ) {
        Ok(definition) => definition,
        Err(error) => {
            tracing::warn!(
                skill_id = %metadata.skill_id,
                path = %skill_file.display(),
                error = %format!("{error:#}"),
                "installed skill package is invalid"
            );
            return unavailable_definition(metadata, SkillUnavailableReason::InvalidPackage);
        }
    };

    definition.identity.skill_id = metadata.skill_id.clone();
    definition.identity.owner = metadata.owner.clone();
    definition.identity.slug = metadata.slug.to_owned();
    definition.identity.source_kind = metadata.source_kind;
    definition.identity.source_root = metadata.source_root.display().to_string();
    definition.identity.skill_dir = metadata.install_path.display().to_string();
    definition.identity.skill_file = skill_file.display().to_string();
    definition.identity.version_hint = metadata.version.clone();
    definition.identity.fingerprint = metadata.fingerprint.to_owned();
    definition.runtime.trust_level = metadata.trust_level;
    definition
}

pub fn load_catalog(params: &SkillCatalogLoadParams) -> Result<SkillCatalogSnapshot> {
    let max_file_bytes = params.max_file_bytes.max(1);
    let mut seen_ids = HashSet::new();
    let mut skills = Vec::with_capacity(params.installations.len() + params.bundled.len());

    for installation in &params.installations {
        if !seen_ids.insert(installation.skill_id.clone()) {
            bail!(
                "duplicate SkillId `{}` in installed catalog",
                installation.skill_id
            );
        }
        let source_root = source_root_for_installation(installation.install_path.as_path());
        let metadata = CatalogMetadata {
            skill_id: &installation.skill_id,
            owner: &installation.owner,
            slug: installation.slug.as_str(),
            version: &installation.version,
            source_kind: installation.source_kind,
            source_root: source_root.as_path(),
            install_path: installation.install_path.as_path(),
            trust_level: installation.trust_level,
            fingerprint: installation.fingerprint.as_str(),
        };
        if installation_is_import_pending(installation) {
            skills.push(unavailable_definition(
                &metadata,
                SkillUnavailableReason::ImportPending,
            ));
        } else {
            skills.push(parse_catalog_entry(&metadata, max_file_bytes));
        }
    }

    for bundled in &params.bundled {
        if !seen_ids.insert(bundled.skill_id.clone()) {
            bail!(
                "duplicate SkillId `{}` in bundled catalog",
                bundled.skill_id
            );
        }
        let version = None;
        // Bundled skills participate in the same exact execution projection
        // contract as installed skills. An empty placeholder passes through
        // catalog resolution but is rejected by the Gateway only after a Turn
        // starts, which makes otherwise valid Composer children fail before
        // their first provider round. Derive the same normalized SKILL.md
        // content hash used by the parser, with a stable unavailable-package
        // identity hash so the catalog never emits an invalid fingerprint.
        let fingerprint = bundled_skill_fingerprint(bundled);
        let metadata = CatalogMetadata {
            skill_id: &bundled.skill_id,
            owner: &bundled.owner,
            slug: bundled.slug.as_str(),
            version: &version,
            source_kind: SkillSourceKind::System,
            source_root: bundled.source_root.as_path(),
            install_path: bundled.install_path.as_path(),
            trust_level: SkillTrustLevel::Internal,
            fingerprint: fingerprint.as_str(),
        };
        skills.push(parse_catalog_entry(&metadata, max_file_bytes));
    }

    skills.sort_by(|left, right| left.identity.skill_id.cmp(&right.identity.skill_id));
    Ok(SkillCatalogSnapshot {
        version: now_version(),
        generated_at_unix: now_unix_seconds(),
        skills,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("valid test SkillId")
    }

    fn temp_case(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pioneer-skills-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn installation(id: char, path: PathBuf) -> SkillCatalogInstallation {
        SkillCatalogInstallation {
            skill_id: skill_id(id),
            owner: Some("same-owner".to_owned()),
            slug: "same-slug".to_owned(),
            version: Some("1.0".to_owned()),
            source_kind: SkillSourceKind::User,
            source_ref: "archive:fixture".to_owned(),
            install_path: path,
            trust_level: SkillTrustLevel::Community,
            fingerprint: "same-fingerprint".to_owned(),
        }
    }

    fn write_package(path: &Path, name: &str) {
        fs::create_dir_all(path).expect("create package");
        fs::write(
            path.join("SKILL.md"),
            format!("---\nname: {name}\nowner: ignored\nslug: ignored\ndescription: Test\n---\nInstructions"),
        )
        .expect("write package");
    }

    #[test]
    fn duplicate_metadata_rows_remain_distinct_by_id() {
        let root = temp_case("duplicates");
        let first = root.join("AAAAAAAAAAAAAAAAAAAAA/same-slug");
        let second = root.join("BBBBBBBBBBBBBBBBBBBBB/same-slug");
        write_package(first.as_path(), "Same");
        write_package(second.as_path(), "Same");
        let catalog = load_catalog(&SkillCatalogLoadParams {
            installations: vec![installation('A', first), installation('B', second)],
            bundled: Vec::new(),
            max_file_bytes: 64 * 1024,
        })
        .expect("catalog should load");
        assert_eq!(catalog.skills.len(), 2);
        assert_eq!(
            catalog.skills[0].identity.owner.as_deref(),
            Some("same-owner")
        );
        assert_eq!(catalog.skills[0].identity.slug, "same-slug");
        assert_ne!(
            catalog.skills[0].identity.skill_id,
            catalog.skills[1].identity.skill_id
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_pending_uses_exact_source_provenance_instead_of_path_shape() {
        let root = temp_case("import-pending-path-shape");
        let id = skill_id('P');
        let external = root.join("external").join(id.as_str()).join("same-shape");
        write_package(external.as_path(), "Pending external package");
        let managed = root.join("managed").join(id.as_str()).join("same-shape");
        write_package(managed.as_path(), "Managed package");

        let pending = SkillCatalogInstallation {
            skill_id: id,
            owner: None,
            slug: "same-shape".to_owned(),
            version: None,
            source_kind: SkillSourceKind::User,
            source_ref: format!("import-path:{}", external.display()),
            install_path: external,
            trust_level: SkillTrustLevel::Community,
            fingerprint: "pending".to_owned(),
        };
        let pending_catalog = load_catalog(&SkillCatalogLoadParams {
            installations: vec![pending.clone()],
            bundled: Vec::new(),
            max_file_bytes: 64 * 1024,
        })
        .expect("pending catalog should load");
        assert_eq!(
            pending_catalog.skills[0].unavailable_reason(),
            Some(SkillUnavailableReason::ImportPending)
        );

        let managed_catalog = load_catalog(&SkillCatalogLoadParams {
            installations: vec![SkillCatalogInstallation {
                install_path: managed,
                ..pending
            }],
            bundled: Vec::new(),
            max_file_bytes: 64 * 1024,
        })
        .expect("managed catalog should load");
        assert!(managed_catalog.skills[0].is_available());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_missing_invalid_and_bundled_rows_keep_their_ids() {
        let root = temp_case("availability");
        let invalid = root.join("CCCCCCCCCCCCCCCCCCCCC/same-slug");
        fs::create_dir_all(invalid.as_path()).expect("create invalid package");
        fs::write(invalid.join("SKILL.md"), "---\nname: Broken\n---\n")
            .expect("write invalid package");
        let bundled_path = root.join("bundled/pioneer/browser");
        write_package(bundled_path.as_path(), "Browser");
        let mut pending = installation('D', root.join("external-source"));
        pending.source_ref = format!("import-path:{}", pending.install_path.display());
        let catalog = load_catalog(&SkillCatalogLoadParams {
            installations: vec![
                installation('C', invalid),
                pending,
                installation('E', root.join("missing")),
            ],
            bundled: vec![BundledSkillCatalogEntry {
                skill_id: skill_id('F'),
                owner: Some("pioneer".to_owned()),
                slug: "browser".to_owned(),
                source_root: root.join("bundled"),
                install_path: bundled_path,
            }],
            max_file_bytes: 64 * 1024,
        })
        .expect("catalog should retain unavailable rows");
        let by_id = catalog
            .skills
            .iter()
            .map(|skill| (skill.identity.skill_id.as_str(), skill))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            by_id[skill_id('C').as_str()].unavailable_reason(),
            Some(SkillUnavailableReason::InvalidPackage)
        );
        assert_eq!(
            by_id[skill_id('D').as_str()].unavailable_reason(),
            Some(SkillUnavailableReason::ImportPending)
        );
        assert_eq!(
            by_id[skill_id('E').as_str()].unavailable_reason(),
            Some(SkillUnavailableReason::MissingPackage)
        );
        assert!(by_id[skill_id('F').as_str()].is_available());
        assert_eq!(
            by_id[skill_id('F').as_str()].identity.owner.as_deref(),
            Some("pioneer")
        );
        let _ = fs::remove_dir_all(root);
    }
}
