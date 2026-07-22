use super::super::upload::ValidatedSkillPackRoot;
use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use pioneer_skills::{
    PrepareMaterializedSkillRequest, PreparedMaterializedSkill, SkillInstallerPolicy,
    SkillSourceKind, prepare_materialized_skill,
};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone)]
pub(super) struct SkillPackMemberCandidate {
    pub pack_member_key: String,
    pub skill_id: SkillId,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedSkillPackMember {
    pub pack_member_key: String,
    pub prepared: PreparedMaterializedSkill,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedSkillPack {
    pub pack_name: String,
    pub members: Vec<PreparedSkillPackMember>,
}

pub(super) fn prepare_skill_pack(
    validated: ValidatedSkillPackRoot,
    candidates: Vec<SkillPackMemberCandidate>,
    source_kind: SkillSourceKind,
    source_ref: String,
    policy: SkillInstallerPolicy,
) -> Result<PreparedSkillPack> {
    let mut candidates_by_key = BTreeMap::new();
    let mut candidate_ids = HashSet::new();
    for candidate in candidates {
        if !candidate_ids.insert(candidate.skill_id.clone()) {
            bail!("pack child candidate SkillId values must be unique");
        }
        if candidates_by_key
            .insert(candidate.pack_member_key.clone(), candidate.skill_id)
            .is_some()
        {
            bail!(
                "duplicate candidate for pack member `{}`",
                candidate.pack_member_key
            );
        }
    }

    let mut prepared_members = Vec::with_capacity(validated.members.len());
    for member in validated.members {
        let skill_id = candidates_by_key
            .remove(member.pack_member_key.as_str())
            .with_context(|| {
                format!(
                    "missing candidate SkillId for pack member `{}`",
                    member.pack_member_key
                )
            })?;
        let prepared = prepare_materialized_skill(PrepareMaterializedSkillRequest {
            skill_id,
            source_kind: source_kind.clone(),
            source_ref: source_ref.clone(),
            materialized_source_path: member.source_dir,
            policy: policy.clone(),
        })
        .with_context(|| format!("failed to prepare pack member `{}`", member.pack_member_key))?;
        prepared_members.push(PreparedSkillPackMember {
            pack_member_key: member.pack_member_key,
            prepared,
        });
    }

    if let Some(unexpected) = candidates_by_key.keys().next() {
        bail!("candidate references unknown pack member `{unexpected}`");
    }

    Ok(PreparedSkillPack {
        pack_name: validated.pack_name,
        members: prepared_members,
    })
}

pub(super) fn reprepare_skill_pack_members(
    prepared_pack: &mut PreparedSkillPack,
    validated: &ValidatedSkillPackRoot,
    candidates: &[SkillPackMemberCandidate],
    member_keys: &[String],
    source_kind: SkillSourceKind,
    source_ref: &str,
    policy: SkillInstallerPolicy,
) -> Result<()> {
    let requested = member_keys
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if requested.len() != member_keys.len() {
        bail!("duplicate pack member requested for re-preparation");
    }

    let candidates_by_key = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.pack_member_key.as_str(),
                candidate.skill_id.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let sources_by_key = validated
        .members
        .iter()
        .map(|member| (member.pack_member_key.as_str(), member.source_dir.clone()))
        .collect::<BTreeMap<_, _>>();

    let mut replacements = Vec::with_capacity(member_keys.len());
    for member_key in member_keys {
        let skill_id = candidates_by_key
            .get(member_key.as_str())
            .with_context(|| format!("missing replacement candidate for `{member_key}`"))?
            .clone();
        let source_dir = sources_by_key
            .get(member_key.as_str())
            .with_context(|| format!("missing source directory for `{member_key}`"))?
            .clone();
        let replacement = prepare_materialized_skill(PrepareMaterializedSkillRequest {
            skill_id,
            source_kind: source_kind.clone(),
            source_ref: source_ref.to_owned(),
            materialized_source_path: source_dir,
            policy: policy.clone(),
        })
        .with_context(|| format!("failed to re-prepare pack member `{member_key}`"))?;
        replacements.push((member_key.as_str(), replacement));
    }

    for (member_key, replacement) in replacements {
        let prepared = prepared_pack
            .members
            .iter_mut()
            .find(|member| member.pack_member_key == member_key)
            .with_context(|| format!("prepared pack has no member `{member_key}`"))?;
        prepared.prepared = replacement;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::skills::upload::validate_skill_pack_root;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prepares_every_member_in_deterministic_member_key_order() {
        let root = temp_case("valid");
        let pack_root = root.join("My Pack");
        write_skill(
            pack_root.join("z-member").as_path(),
            "Zulu",
            "z description",
        );
        write_skill(
            pack_root.join("a-member").as_path(),
            "Alpha",
            "a description",
        );
        fs::create_dir_all(pack_root.join("z-member/assets/nested")).expect("create asset");
        fs::write(pack_root.join("z-member/assets/nested/file.txt"), b"asset")
            .expect("write asset");
        let validated = validate_skill_pack_root(pack_root.clone()).expect("validate pack");

        let prepared = prepare_skill_pack(
            validated,
            vec![
                candidate("z-member", "ZZZZZZZZZZZZZZZZZZZZZ"),
                candidate("a-member", "AAAAAAAAAAAAAAAAAAAAA"),
            ],
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect("prepare pack");

        assert_eq!(prepared.pack_name, "My Pack");
        assert_eq!(
            prepared
                .members
                .iter()
                .map(|member| member.pack_member_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a-member", "z-member"]
        );
        assert_eq!(
            prepared.members[0]
                .prepared
                .definition
                .identity
                .skill_id
                .as_str(),
            "AAAAAAAAAAAAAAAAAAAAA"
        );
        assert!(pack_root.join("z-member/assets/nested/file.txt").is_file());
        assert!(!root.join("skills-lock.toml").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collision_retry_reprepares_only_the_affected_member() {
        let root = temp_case("collision-retry");
        let pack_root = root.join("pack");
        write_skill(pack_root.join("alpha").as_path(), "Alpha", "description");
        write_skill(pack_root.join("zeta").as_path(), "Zeta", "description");
        let validated = validate_skill_pack_root(pack_root).expect("validate pack");
        let mut candidates = vec![
            candidate("alpha", "AAAAAAAAAAAAAAAAAAAAA"),
            candidate("zeta", "ZZZZZZZZZZZZZZZZZZZZZ"),
        ];
        let mut prepared = prepare_skill_pack(
            validated.clone(),
            candidates.clone(),
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect("prepare pack");

        candidates[0].skill_id = SkillId::new("BBBBBBBBBBBBBBBBBBBBB").expect("replacement id");
        reprepare_skill_pack_members(
            &mut prepared,
            &validated,
            candidates.as_slice(),
            &["alpha".to_owned()],
            SkillSourceKind::User,
            "upload:pack",
            SkillInstallerPolicy::default(),
        )
        .expect("re-prepare collided child");

        assert_eq!(
            prepared.members[0]
                .prepared
                .definition
                .identity
                .skill_id
                .as_str(),
            "BBBBBBBBBBBBBBBBBBBBB"
        );
        assert_eq!(
            prepared.members[1]
                .prepared
                .definition
                .identity
                .skill_id
                .as_str(),
            "ZZZZZZZZZZZZZZZZZZZZZ"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_second_member_fails_before_any_publication() {
        let root = temp_case("invalid-second");
        let pack_root = root.join("pack");
        write_skill(
            pack_root.join("a-valid").as_path(),
            "Valid",
            "valid description",
        );
        fs::create_dir_all(pack_root.join("b-invalid")).expect("create invalid member");
        fs::write(pack_root.join("b-invalid/SKILL.md"), [0xff, 0xfe, 0xfd])
            .expect("write invalid skill");
        let validated = validate_skill_pack_root(pack_root.clone()).expect("validate pack shape");

        let error = prepare_skill_pack(
            validated,
            vec![
                candidate("a-valid", "AAAAAAAAAAAAAAAAAAAAA"),
                candidate("b-invalid", "BBBBBBBBBBBBBBBBBBBBB"),
            ],
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect_err("invalid child should reject the whole prepared pack")
        .to_string();

        assert!(error.contains("b-invalid"), "{error}");
        assert!(pack_root.join("a-valid/SKILL.md").is_file());
        assert!(!root.join("skills-lock.toml").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preparation_rejects_missing_unknown_duplicate_and_reused_candidates() {
        let make_validated = || {
            let root = temp_case("candidate-validation");
            let pack_root = root.join("pack");
            write_skill(pack_root.join("member").as_path(), "Member", "description");
            (
                root,
                validate_skill_pack_root(pack_root).expect("validate pack"),
            )
        };

        let (root, validated) = make_validated();
        let missing = prepare_skill_pack(
            validated,
            Vec::new(),
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect_err("missing candidate should fail")
        .to_string();
        assert!(missing.contains("member"));
        let _ = fs::remove_dir_all(root);

        let (root, validated) = make_validated();
        let unknown = prepare_skill_pack(
            validated,
            vec![
                candidate("member", "AAAAAAAAAAAAAAAAAAAAA"),
                candidate("unknown", "BBBBBBBBBBBBBBBBBBBBB"),
            ],
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect_err("unknown candidate should fail")
        .to_string();
        assert!(unknown.contains("unknown"));
        let _ = fs::remove_dir_all(root);

        let (root, validated) = make_validated();
        let duplicate = prepare_skill_pack(
            validated,
            vec![
                candidate("member", "AAAAAAAAAAAAAAAAAAAAA"),
                candidate("member", "BBBBBBBBBBBBBBBBBBBBB"),
            ],
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect_err("duplicate member candidate should fail")
        .to_string();
        assert!(duplicate.contains("duplicate candidate"));
        let _ = fs::remove_dir_all(root);

        let root = temp_case("reused-id");
        let pack_root = root.join("pack");
        write_skill(pack_root.join("first").as_path(), "First", "description");
        write_skill(pack_root.join("second").as_path(), "Second", "description");
        let validated = validate_skill_pack_root(pack_root).expect("validate pack");
        let reused = prepare_skill_pack(
            validated,
            vec![
                candidate("first", "AAAAAAAAAAAAAAAAAAAAA"),
                candidate("second", "AAAAAAAAAAAAAAAAAAAAA"),
            ],
            SkillSourceKind::User,
            "upload:pack".to_owned(),
            SkillInstallerPolicy::default(),
        )
        .expect_err("reused SkillId should fail")
        .to_string();
        assert!(reused.contains("SkillId values must be unique"));
        let _ = fs::remove_dir_all(root);
    }

    fn candidate(pack_member_key: &str, skill_id: &str) -> SkillPackMemberCandidate {
        SkillPackMemberCandidate {
            pack_member_key: pack_member_key.to_owned(),
            skill_id: SkillId::new(skill_id).expect("valid test SkillId"),
        }
    }

    fn write_skill(dir: &Path, name: &str, description: &str) {
        fs::create_dir_all(dir).expect("create skill directory");
        let slug = name.to_ascii_lowercase().replace(' ', "-");
        fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\nslug: {slug}\ndescription: {description}\n---\nBody"),
        )
        .expect("write skill");
    }

    fn temp_case(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("pioneer-pack-prepare-{name}-{nanos}"));
        fs::create_dir_all(root.as_path()).expect("create temp root");
        root
    }
}
