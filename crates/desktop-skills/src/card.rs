use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::skills::{presentation as skill_presentation, types::SkillListItem};

/// Shared catalogue card. Navigation and domain commands belong to the caller.
#[derive(gpui_kit::IntoElement)]
pub struct SkillCard {
    title: String,
    owner: Option<String>,
    description: String,
    version: String,
    status_label: String,
    status_color: Hsla,
    pending: bool,
}
impl SkillCard {
    pub const HEIGHT: f32 = pioneer_desktop_foundation::CatalogCard::HEIGHT;

    pub fn new(skill: &SkillListItem, cx: &mut App) -> Self {
        let summary = skill_presentation::skill_summary_presentation(skill);
        Self {
            title: skill.slug.clone(),
            owner: summary.slug.owner.clone(),
            description: skill.description.clone(),
            version: summary.version.clone().unwrap_or_else(|| "-".into()),
            status_label: Self::status_label(&summary.status),
            status_color: Self::status_color(summary.status_tone, cx),
            pending: false,
        }
    }

    /// Preserve the card shape for a component without an installed native record.
    pub fn missing(title: String, status_label: String, status_color: Hsla) -> Self {
        Self {
            title,
            owner: None,
            description: String::new(),
            version: "-".into(),
            status_label,
            status_color,
            pending: false,
        }
    }
    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
        self
    }

    pub fn status(mut self, label: String, color: Hsla) -> Self {
        self.status_label = label;
        self.status_color = color;
        self
    }

    pub(crate) fn status_label(status: &skill_presentation::SkillStatus) -> String {
        match status {
            skill_presentation::SkillStatus::Active => t!("skills.status.active").to_string(),
            skill_presentation::SkillStatus::Blocked => t!("skills.status.blocked").to_string(),
            skill_presentation::SkillStatus::Disabled => t!("skills.status.disabled").to_string(),
            skill_presentation::SkillStatus::Other(value) => value.clone(),
        }
    }

    pub(crate) fn status_color(
        tone: skill_presentation::SkillDiagnosticsTone,
        cx: &mut App,
    ) -> Hsla {
        match tone {
            skill_presentation::SkillDiagnosticsTone::Success => cx.theme().success,
            skill_presentation::SkillDiagnosticsTone::Warning => cx.theme().warning,
            _ => cx.theme().foreground.opacity(1.),
        }
    }
}
impl RenderOnce for SkillCard {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        pioneer_desktop_foundation::CatalogCard::new(self.title)
            .when_some(self.owner, |card, owner| card.owner(owner))
            .description(self.description)
            .metadata(format!("{} {}", t!("skills.card.version"), self.version))
            .status(self.status_label, self.status_color)
            .pending(self.pending)
    }
}
