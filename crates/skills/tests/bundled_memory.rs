use pioneer_skills::{SkillId, SkillSourceKind, parse_skill_from_file};

#[test]
fn bundled_memory_skill_loads_with_the_pioneer_contract() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../resources/skills");
    let path = root.join("pioneer/memory/SKILL.md");
    let skill = parse_skill_from_file(
        SkillId::new("EEEEEEEEEEEEEEEEEEEEE").unwrap(),
        &path,
        SkillSourceKind::System,
        &root,
        1024 * 1024,
    )
    .expect("bundled memory skill must load with Pioneer frontmatter");
    assert_eq!(skill.identity.slug, "memory");
    for reference in [
        "tool-schemas.md",
        "scopes-categories-keys.md",
        "workflows-and-examples.md",
        "troubleshooting.md",
    ] {
        let content =
            std::fs::read_to_string(path.parent().unwrap().join("references").join(reference))
                .unwrap();
        assert!(!content.trim().is_empty());
    }
}
