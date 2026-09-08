use pioneer_skills::{SkillId, SkillSourceKind, parse_skill_from_file};

#[test]
fn task_and_subagent_skills_load_with_native_frontmatter_and_local_references() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../resources/skills");
    for slug in ["tasks", "subagents"] {
        let path = root.join("pioneer").join(slug).join("SKILL.md");
        let skill = parse_skill_from_file(
            SkillId::new("EEEEEEEEEEEEEEEEEEEEE").unwrap(),
            &path,
            SkillSourceKind::System,
            &root,
            1024 * 1024,
        )
        .expect("bundled task skill must load");
        assert_eq!(skill.identity.slug, slug);
        for reference in ["tool-schemas.md", "troubleshooting.md"] {
            let body =
                std::fs::read_to_string(path.parent().unwrap().join("references").join(reference))
                    .unwrap();
            assert!(!body.trim().is_empty());
        }
    }
}
