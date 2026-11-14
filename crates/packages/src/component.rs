use crate::PackageMcpServer;
use pioneer_skills::contract::parse_skill_markdown;
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
        server: PackageMcpServer,
    },
}
pub fn valid_skill_at(text: &str, name: &str, member_path: &str) -> bool {
    let Ok(skill_id) = SkillId::new("000000000000000000000") else {
        return false;
    };
    parse_skill_markdown(
        text,
        SkillMarkdownParseContext {
            skill_id,
            source_kind: SkillSourceKind::User,
            source_root: String::new(),
            skill_dir: member_path.into(),
            skill_file: format!("{member_path}/SKILL.md"),
            parent_directory_name: name.into(),
            identity_owner_override: None,
            identity_slug_override: None,
            version_hint_override: None,
            display_name_override: None,
        },
    )
    .is_ok()
}
