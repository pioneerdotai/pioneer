use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillErrorKind {
    InvalidSkill,
    InvalidFrontmatter,
    InvalidRuntime,
    Io,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillError {
    pub kind: SkillErrorKind,
    pub message: String,
}

impl SkillError {
    pub fn invalid_skill(message: impl Into<String>) -> Self {
        Self {
            kind: SkillErrorKind::InvalidSkill,
            message: message.into(),
        }
    }

    pub fn invalid_frontmatter(message: impl Into<String>) -> Self {
        Self {
            kind: SkillErrorKind::InvalidFrontmatter,
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self {
            kind: SkillErrorKind::Io,
            message: message.into(),
        }
    }

    pub fn invalid_runtime(message: impl Into<String>) -> Self {
        Self {
            kind: SkillErrorKind::InvalidRuntime,
            message: message.into(),
        }
    }
}

impl Display for SkillError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SkillError {}
