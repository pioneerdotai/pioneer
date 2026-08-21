//! CLI runtime provider settings planning.

use crate::settings::gateway::GatewaySettingsUpdatePlan;
use pioneer_protocol::{
    CLIAgentRuntimeKind, GatewayCliRuntimeInstanceSettings, GatewayCliRuntimeSettings,
    GatewaySettingsSnapshot, GatewaySettingsUpdate,
};
use std::collections::HashSet;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CLIRuntimeProviderDraftMode {
    Create,
    Edit { original_id: String },
    Duplicate { source_id: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CLIRuntimeProviderDraftField {
    Id,
    DisplayName,
    BinaryPath,
    HomePath,
    ShadowHomePath,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CLIRuntimeProviderDraft {
    pub mode: CLIRuntimeProviderDraftMode,
    pub kind: CLIAgentRuntimeKind,
    pub id: String,
    pub display_name: String,
    /// Stable agent presentation nickname.  The UI does not expose this
    /// field yet, but edits must preserve the gateway-owned value.
    #[serde(default)]
    pub nickname: String,
    pub enabled: bool,
    pub binary_path: String,
    pub home_path: String,
    pub shadow_home_path: String,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CLIRuntimeProviderSettingsRejection {
    MissingSettings,
    MissingRuntime { runtime_id: String },
    EmptyId,
    InvalidId { id: String, message: String },
    DuplicateId { id: String },
    DuplicateDisplayName { display_name: String },
    EmptyPath { field: String },
    InvalidPath { field: String, message: String },
    ShadowHomeMatchesHome,
    UnsupportedKind { kind: CLIAgentRuntimeKind },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CLIRuntimeProviderSettingsPlan {
    Send(GatewaySettingsUpdatePlan),
    Reject(CLIRuntimeProviderSettingsRejection),
}

impl CLIRuntimeProviderDraft {
    pub fn create_for_kind(
        current: Option<&GatewaySettingsSnapshot>,
        kind: CLIAgentRuntimeKind,
    ) -> Self {
        let defaults = cli_runtime_provider_kind_defaults(kind);
        let id = next_available_cli_runtime_id(current, defaults.id_base);
        Self {
            mode: CLIRuntimeProviderDraftMode::Create,
            kind,
            display_name: next_available_cli_runtime_display_name(current, defaults.display_name),
            nickname: id.clone(),
            id,
            enabled: true,
            binary_path: defaults.binary_path.to_owned(),
            home_path: defaults.home_path.to_owned(),
            shadow_home_path: String::new(),
        }
    }

    pub fn edit(instance: &GatewayCliRuntimeInstanceSettings) -> Self {
        Self {
            mode: CLIRuntimeProviderDraftMode::Edit {
                original_id: instance.id.clone(),
            },
            kind: instance.kind,
            id: instance.id.clone(),
            display_name: instance.display_name.clone(),
            nickname: if instance.nickname.trim().is_empty() {
                instance.id.clone()
            } else {
                instance.nickname.clone()
            },
            enabled: instance.enabled,
            binary_path: instance.binary_path.clone(),
            home_path: instance.home_path.clone(),
            shadow_home_path: instance.shadow_home_path.clone().unwrap_or_default(),
        }
    }

    pub fn duplicate(
        current: Option<&GatewaySettingsSnapshot>,
        instance: &GatewayCliRuntimeInstanceSettings,
    ) -> Self {
        let id = next_available_cli_runtime_id(current, format!("{}_copy", instance.id).as_str());
        let source_nickname = if instance.nickname.trim().is_empty() {
            instance.id.as_str()
        } else {
            instance.nickname.as_str()
        };
        Self {
            mode: CLIRuntimeProviderDraftMode::Duplicate {
                source_id: instance.id.clone(),
            },
            kind: instance.kind,
            id,
            display_name: next_available_cli_runtime_display_name(
                current,
                format!("{} Copy", instance.display_name).as_str(),
            ),
            nickname: format!("{source_nickname}_copy"),
            enabled: instance.enabled,
            binary_path: instance.binary_path.clone(),
            home_path: instance.home_path.clone(),
            shadow_home_path: instance.shadow_home_path.clone().unwrap_or_default(),
        }
    }

    pub fn set_text_field(&mut self, field: CLIRuntimeProviderDraftField, value: String) {
        match field {
            CLIRuntimeProviderDraftField::Id => self.id = value,
            CLIRuntimeProviderDraftField::DisplayName => self.display_name = value,
            CLIRuntimeProviderDraftField::BinaryPath => self.binary_path = value,
            CLIRuntimeProviderDraftField::HomePath => self.home_path = value,
            CLIRuntimeProviderDraftField::ShadowHomePath => self.shadow_home_path = value,
        }
    }
}

pub fn plan_cli_runtime_provider_draft_update(
    current: Option<&GatewaySettingsSnapshot>,
    draft: &CLIRuntimeProviderDraft,
) -> CLIRuntimeProviderSettingsPlan {
    let Some(current) = current else {
        return CLIRuntimeProviderSettingsPlan::Reject(
            CLIRuntimeProviderSettingsRejection::MissingSettings,
        );
    };

    match cli_runtime_provider_instances_with_draft(current, draft) {
        Ok(instances) => cli_runtime_provider_settings_plan(current, instances),
        Err(rejection) => CLIRuntimeProviderSettingsPlan::Reject(rejection),
    }
}

pub fn plan_cli_runtime_provider_enabled_update(
    current: Option<&GatewaySettingsSnapshot>,
    runtime_id: &str,
    enabled: bool,
) -> CLIRuntimeProviderSettingsPlan {
    let Some(current) = current else {
        return CLIRuntimeProviderSettingsPlan::Reject(
            CLIRuntimeProviderSettingsRejection::MissingSettings,
        );
    };

    let mut found = false;
    let mut instances = current.cli_runtimes.instances.clone();
    for instance in &mut instances {
        if instance.id == runtime_id {
            instance.enabled = enabled;
            found = true;
            break;
        }
    }

    if !found {
        return CLIRuntimeProviderSettingsPlan::Reject(
            CLIRuntimeProviderSettingsRejection::MissingRuntime {
                runtime_id: runtime_id.to_owned(),
            },
        );
    }

    match validate_cli_runtime_provider_instances(instances.as_slice()) {
        Ok(()) => cli_runtime_provider_settings_plan(current, instances),
        Err(rejection) => CLIRuntimeProviderSettingsPlan::Reject(rejection),
    }
}

pub fn find_cli_runtime_provider_instance<'a>(
    current: Option<&'a GatewaySettingsSnapshot>,
    runtime_id: &str,
) -> Option<&'a GatewayCliRuntimeInstanceSettings> {
    current?
        .cli_runtimes
        .instances
        .iter()
        .find(|instance| instance.id == runtime_id)
}

pub fn cli_runtime_provider_settings_rejection_message(
    rejection: &CLIRuntimeProviderSettingsRejection,
) -> String {
    match rejection {
        CLIRuntimeProviderSettingsRejection::MissingSettings => {
            "Gateway settings are not loaded".to_owned()
        }
        CLIRuntimeProviderSettingsRejection::MissingRuntime { runtime_id } => {
            format!("CLI provider `{runtime_id}` was not found")
        }
        CLIRuntimeProviderSettingsRejection::EmptyId => "CLI provider id is required".to_owned(),
        CLIRuntimeProviderSettingsRejection::InvalidId { message, .. } => message.clone(),
        CLIRuntimeProviderSettingsRejection::DuplicateId { id } => {
            format!("CLI provider id `{id}` is already used")
        }
        CLIRuntimeProviderSettingsRejection::DuplicateDisplayName { display_name } => {
            format!("CLI provider display name `{display_name}` is already used")
        }
        CLIRuntimeProviderSettingsRejection::EmptyPath { field } => {
            format!("CLI provider `{field}` is required")
        }
        CLIRuntimeProviderSettingsRejection::InvalidPath { message, .. } => message.clone(),
        CLIRuntimeProviderSettingsRejection::ShadowHomeMatchesHome => {
            "Shadow home must differ from home".to_owned()
        }
        CLIRuntimeProviderSettingsRejection::UnsupportedKind { kind } => {
            format!("CLI provider kind cannot be changed from `{kind:?}` while editing")
        }
    }
}

fn cli_runtime_provider_instances_with_draft(
    current: &GatewaySettingsSnapshot,
    draft: &CLIRuntimeProviderDraft,
) -> Result<Vec<GatewayCliRuntimeInstanceSettings>, CLIRuntimeProviderSettingsRejection> {
    let replacement = cli_runtime_provider_instance_from_draft(draft)?;
    let mut instances = current.cli_runtimes.instances.clone();

    match &draft.mode {
        CLIRuntimeProviderDraftMode::Create | CLIRuntimeProviderDraftMode::Duplicate { .. } => {
            instances.push(replacement);
        }
        CLIRuntimeProviderDraftMode::Edit { original_id } => {
            let Some(index) = instances
                .iter()
                .position(|instance| instance.id == *original_id)
            else {
                return Err(CLIRuntimeProviderSettingsRejection::MissingRuntime {
                    runtime_id: original_id.clone(),
                });
            };
            if instances[index].kind != replacement.kind {
                return Err(CLIRuntimeProviderSettingsRejection::UnsupportedKind {
                    kind: instances[index].kind,
                });
            }
            instances[index] = replacement;
        }
    }

    validate_cli_runtime_provider_instances(instances.as_slice())?;
    Ok(instances)
}

fn cli_runtime_provider_settings_plan(
    current: &GatewaySettingsSnapshot,
    instances: Vec<GatewayCliRuntimeInstanceSettings>,
) -> CLIRuntimeProviderSettingsPlan {
    let cli_runtimes = GatewayCliRuntimeSettings { instances };
    let mut snapshot = current.clone();
    snapshot.cli_runtimes = cli_runtimes.clone();
    CLIRuntimeProviderSettingsPlan::Send(GatewaySettingsUpdatePlan {
        snapshot,
        update: GatewaySettingsUpdate {
            general: None,
            memory: None,
            self_improvement: None,
            thread_episodic: None,
            cli_runtimes: Some(cli_runtimes),
            remote_access: None,
            voice_input: None,
        },
    })
}

fn cli_runtime_provider_instance_from_draft(
    draft: &CLIRuntimeProviderDraft,
) -> Result<GatewayCliRuntimeInstanceSettings, CLIRuntimeProviderSettingsRejection> {
    let id = normalize_cli_runtime_provider_id(draft.id.as_str())?;
    let display_name =
        normalize_cli_runtime_provider_display_name(draft.display_name.as_str(), id.as_str())?;
    let binary_path =
        normalize_cli_runtime_provider_required_path("binary_path", draft.binary_path.as_str())?;
    let home_path =
        normalize_cli_runtime_provider_required_path("home_path", draft.home_path.as_str())?;
    let shadow_home_path = normalize_cli_runtime_provider_optional_path(
        "shadow_home_path",
        draft.shadow_home_path.as_str(),
    )?;
    if shadow_home_path.as_deref() == Some(home_path.as_str()) {
        return Err(CLIRuntimeProviderSettingsRejection::ShadowHomeMatchesHome);
    }
    let nickname = if draft.nickname.trim().is_empty() {
        id.clone()
    } else {
        draft.nickname.trim().to_owned()
    };

    Ok(GatewayCliRuntimeInstanceSettings {
        id,
        kind: draft.kind,
        display_name,
        nickname,
        enabled: draft.enabled,
        binary_path,
        home_path,
        shadow_home_path,
    })
}

fn validate_cli_runtime_provider_instances(
    instances: &[GatewayCliRuntimeInstanceSettings],
) -> Result<(), CLIRuntimeProviderSettingsRejection> {
    let mut ids = HashSet::new();
    let mut display_names = HashSet::new();

    for instance in instances {
        let id = normalize_cli_runtime_provider_id(instance.id.as_str())?;
        if !ids.insert(id.clone()) {
            return Err(CLIRuntimeProviderSettingsRejection::DuplicateId { id });
        }

        let display_name = normalize_cli_runtime_provider_display_name(
            instance.display_name.as_str(),
            id.as_str(),
        )?;
        if !display_names.insert(display_name.to_ascii_lowercase()) {
            return Err(CLIRuntimeProviderSettingsRejection::DuplicateDisplayName { display_name });
        }

        normalize_cli_runtime_provider_required_path("binary_path", instance.binary_path.as_str())?;
        let home_path =
            normalize_cli_runtime_provider_required_path("home_path", instance.home_path.as_str())?;
        let shadow_home_path = normalize_cli_runtime_provider_optional_path(
            "shadow_home_path",
            instance.shadow_home_path.as_deref().unwrap_or_default(),
        )?;
        if shadow_home_path.as_deref() == Some(home_path.as_str()) {
            return Err(CLIRuntimeProviderSettingsRejection::ShadowHomeMatchesHome);
        }
    }

    Ok(())
}

fn normalize_cli_runtime_provider_id(
    raw: &str,
) -> Result<String, CLIRuntimeProviderSettingsRejection> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CLIRuntimeProviderSettingsRejection::EmptyId);
    }

    let mut normalized = String::new();
    let mut previous_separator = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if ch == '_' || ch == '-' || ch == '.' || ch.is_ascii_whitespace() {
            if !normalized.is_empty() && !previous_separator {
                normalized.push('_');
                previous_separator = true;
            }
        } else {
            return Err(CLIRuntimeProviderSettingsRejection::InvalidId {
                id: raw.to_owned(),
                message: format!("CLI provider id `{raw}` contains unsupported character `{ch}`"),
            });
        }
    }

    let normalized = normalized.trim_matches('_').to_owned();
    if normalized.is_empty() {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidId {
            id: raw.to_owned(),
            message: format!("CLI provider id `{raw}` must contain an ASCII letter or digit"),
        });
    }
    if normalized.chars().count() > 64 {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidId {
            id: raw.to_owned(),
            message: format!("CLI provider id `{raw}` must be at most 64 characters"),
        });
    }
    Ok(normalized)
}

fn normalize_cli_runtime_provider_display_name(
    raw: &str,
    id: &str,
) -> Result<String, CLIRuntimeProviderSettingsRejection> {
    let trimmed = raw.trim();
    let display_name = if trimmed.is_empty() {
        cli_runtime_display_name_from_id(id)
    } else {
        trimmed.to_owned()
    };
    if display_name.chars().count() > 80 {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidPath {
            field: "display_name".to_owned(),
            message: format!(
                "CLI provider display name `{display_name}` must be at most 80 characters"
            ),
        });
    }
    if display_name.chars().any(is_disallowed_settings_text_char) {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidPath {
            field: "display_name".to_owned(),
            message: format!(
                "CLI provider display name `{display_name}` contains unsupported control characters"
            ),
        });
    }
    Ok(display_name)
}

fn normalize_cli_runtime_provider_required_path(
    field: &str,
    raw: &str,
) -> Result<String, CLIRuntimeProviderSettingsRejection> {
    let Some(value) = normalize_cli_runtime_provider_optional_path(field, raw)? else {
        return Err(CLIRuntimeProviderSettingsRejection::EmptyPath {
            field: field.to_owned(),
        });
    };
    Ok(value)
}

fn normalize_cli_runtime_provider_optional_path(
    field: &str,
    raw: &str,
) -> Result<Option<String>, CLIRuntimeProviderSettingsRejection> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > 512 {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidPath {
            field: field.to_owned(),
            message: format!("CLI provider `{field}` must be at most 512 characters"),
        });
    }
    if trimmed.chars().any(is_disallowed_settings_text_char) {
        return Err(CLIRuntimeProviderSettingsRejection::InvalidPath {
            field: field.to_owned(),
            message: format!("CLI provider `{field}` contains unsupported control characters"),
        });
    }
    Ok(Some(trimmed.to_owned()))
}

fn is_disallowed_settings_text_char(ch: char) -> bool {
    ch == '\0' || ch == '\n' || ch == '\r' || ch.is_control()
}

fn next_available_cli_runtime_id(current: Option<&GatewaySettingsSnapshot>, base: &str) -> String {
    let normalized_base =
        normalize_cli_runtime_provider_id(base).unwrap_or_else(|_| "cli_runtime".to_owned());
    let Some(current) = current else {
        return normalized_base;
    };
    let ids = current
        .cli_runtimes
        .instances
        .iter()
        .map(|instance| instance.id.as_str())
        .collect::<HashSet<_>>();
    if !ids.contains(normalized_base.as_str()) {
        return normalized_base;
    }
    for suffix in 2..=999 {
        let candidate = format!("{normalized_base}_{suffix}");
        if !ids.contains(candidate.as_str()) {
            return candidate;
        }
    }
    format!(
        "{}_{}",
        normalized_base,
        current.cli_runtimes.instances.len() + 1
    )
}

fn next_available_cli_runtime_display_name(
    current: Option<&GatewaySettingsSnapshot>,
    base: &str,
) -> String {
    let base = base.trim();
    let base = if base.is_empty() { "CLI Runtime" } else { base };
    let Some(current) = current else {
        return base.to_owned();
    };
    let names = current
        .cli_runtimes
        .instances
        .iter()
        .map(|instance| instance.display_name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    if !names.contains(&base.to_ascii_lowercase()) {
        return base.to_owned();
    }
    for suffix in 2..=999 {
        let candidate = format!("{base} {suffix}");
        if !names.contains(&candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    format!("{} {}", base, current.cli_runtimes.instances.len() + 1)
}

#[derive(Clone, Copy)]
struct CLIRuntimeProviderKindDefaults {
    id_base: &'static str,
    kind_label: &'static str,
    display_name: &'static str,
    binary_path: &'static str,
    home_path: &'static str,
    shadow_home_placeholder: &'static str,
}

pub fn cli_runtime_provider_default_display_name(kind: CLIAgentRuntimeKind) -> &'static str {
    cli_runtime_provider_kind_defaults(kind).display_name
}

pub fn cli_runtime_provider_default_binary_path(kind: CLIAgentRuntimeKind) -> &'static str {
    cli_runtime_provider_kind_defaults(kind).binary_path
}

pub fn cli_runtime_provider_default_home_path(kind: CLIAgentRuntimeKind) -> &'static str {
    cli_runtime_provider_kind_defaults(kind).home_path
}

pub fn cli_runtime_provider_kind_label(kind: CLIAgentRuntimeKind) -> &'static str {
    cli_runtime_provider_kind_defaults(kind).kind_label
}

pub fn cli_runtime_provider_default_shadow_home_placeholder(
    kind: CLIAgentRuntimeKind,
) -> &'static str {
    cli_runtime_provider_kind_defaults(kind).shadow_home_placeholder
}

pub const CLI_RUNTIME_PROVIDER_SUPPORTED_KINDS: [CLIAgentRuntimeKind; 2] =
    [CLIAgentRuntimeKind::Codex, CLIAgentRuntimeKind::Claude];

fn cli_runtime_provider_kind_defaults(kind: CLIAgentRuntimeKind) -> CLIRuntimeProviderKindDefaults {
    match kind {
        CLIAgentRuntimeKind::Codex => CLIRuntimeProviderKindDefaults {
            id_base: "codex",
            kind_label: "Codex",
            display_name: "Codex",
            binary_path: "codex",
            home_path: "~/.codex",
            shadow_home_placeholder: "~/.pioneer/codex-work",
        },
        CLIAgentRuntimeKind::Claude => CLIRuntimeProviderKindDefaults {
            id_base: "claude",
            kind_label: "Claude",
            display_name: "Claude",
            binary_path: "claude",
            home_path: "~/.claude",
            shadow_home_placeholder: "~/.pioneer/claude-work",
        },
    }
}

fn cli_runtime_display_name_from_id(id: &str) -> String {
    id.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut word = String::new();
            word.push(first.to_ascii_uppercase());
            word.push_str(chars.as_str());
            word
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{GatewayGeneralSettings, GatewayMemorySettings};

    fn snapshot(instances: Vec<GatewayCliRuntimeInstanceSettings>) -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: GatewayGeneralSettings::default(),
            memory: GatewayMemorySettings::default(),
            self_improvement: Default::default(),
            thread_episodic: Default::default(),
            cli_runtimes: GatewayCliRuntimeSettings { instances },
            remote_access: Default::default(),
            voice_input: Default::default(),
        }
    }

    fn codex_instance(id: &str, display_name: &str) -> GatewayCliRuntimeInstanceSettings {
        GatewayCliRuntimeInstanceSettings {
            id: id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: display_name.to_owned(),
            nickname: id.to_owned(),
            enabled: true,
            binary_path: "codex".to_owned(),
            home_path: "~/.codex".to_owned(),
            shadow_home_path: None,
        }
    }

    #[test]
    fn create_draft_for_kind_uses_runtime_specific_defaults() {
        let current = snapshot(vec![codex_instance("codex", "Codex")]);
        let draft =
            CLIRuntimeProviderDraft::create_for_kind(Some(&current), CLIAgentRuntimeKind::Claude);

        assert_eq!(draft.kind, CLIAgentRuntimeKind::Claude);
        assert_eq!(draft.id, "claude");
        assert_eq!(draft.display_name, "Claude");
        assert_eq!(draft.binary_path, "claude");
        assert_eq!(draft.home_path, "~/.claude");
        assert!(draft.shadow_home_path.is_empty());
    }

    #[test]
    fn create_draft_plan_builds_gateway_settings_update() {
        let current = snapshot(vec![codex_instance("codex", "Codex")]);
        let mut draft =
            CLIRuntimeProviderDraft::create_for_kind(Some(&current), CLIAgentRuntimeKind::Codex);
        assert_eq!(draft.id, "codex_2");
        assert_eq!(draft.display_name, "Codex 2");
        draft.display_name = "Codex Work".to_owned();
        draft.home_path = "~/.codex-work".to_owned();

        let CLIRuntimeProviderSettingsPlan::Send(plan) =
            plan_cli_runtime_provider_draft_update(Some(&current), &draft)
        else {
            panic!("expected send plan");
        };

        assert_eq!(plan.snapshot.cli_runtimes.instances.len(), 2);
        assert_eq!(plan.snapshot.cli_runtimes.instances[1].id, "codex_2");
        assert_eq!(
            plan.update
                .cli_runtimes
                .as_ref()
                .expect("update should include CLI runtimes")
                .instances
                .len(),
            2
        );
        assert!(plan.update.general.is_none());
        assert!(plan.update.memory.is_none());
        assert!(plan.update.thread_episodic.is_none());
    }

    #[test]
    fn edit_draft_replaces_original_even_when_id_changes() {
        let current = snapshot(vec![codex_instance("codex", "Codex CLI")]);
        let mut draft = CLIRuntimeProviderDraft::edit(&current.cli_runtimes.instances[0]);
        draft.id = "Codex Work".to_owned();
        draft.display_name = "Codex Work".to_owned();
        draft.home_path = "~/.codex-work".to_owned();

        let CLIRuntimeProviderSettingsPlan::Send(plan) =
            plan_cli_runtime_provider_draft_update(Some(&current), &draft)
        else {
            panic!("expected send plan");
        };

        assert_eq!(plan.snapshot.cli_runtimes.instances.len(), 1);
        assert_eq!(plan.snapshot.cli_runtimes.instances[0].id, "codex_work");
    }

    #[test]
    fn duplicate_draft_uses_available_id_and_name() {
        let current = snapshot(vec![
            codex_instance("codex", "Codex CLI"),
            codex_instance("codex_copy", "Codex CLI Copy"),
        ]);
        let draft =
            CLIRuntimeProviderDraft::duplicate(Some(&current), &current.cli_runtimes.instances[0]);

        assert_eq!(draft.id, "codex_copy_2");
        assert_eq!(draft.display_name, "Codex CLI Copy 2");
    }

    #[test]
    fn draft_plan_rejects_duplicate_id_display_name_and_invalid_paths() {
        let current = snapshot(vec![codex_instance("codex", "Codex CLI")]);
        let mut draft =
            CLIRuntimeProviderDraft::create_for_kind(Some(&current), CLIAgentRuntimeKind::Codex);
        draft.id = "codex".to_owned();
        assert!(matches!(
            plan_cli_runtime_provider_draft_update(Some(&current), &draft),
            CLIRuntimeProviderSettingsPlan::Reject(
                CLIRuntimeProviderSettingsRejection::DuplicateId { .. }
            )
        ));

        draft.id = "codex_work".to_owned();
        draft.display_name = "codex cli".to_owned();
        assert!(matches!(
            plan_cli_runtime_provider_draft_update(Some(&current), &draft),
            CLIRuntimeProviderSettingsPlan::Reject(
                CLIRuntimeProviderSettingsRejection::DuplicateDisplayName { .. }
            )
        ));

        draft.display_name = "Codex Work".to_owned();
        draft.binary_path = "codex\nbad".to_owned();
        assert!(matches!(
            plan_cli_runtime_provider_draft_update(Some(&current), &draft),
            CLIRuntimeProviderSettingsPlan::Reject(
                CLIRuntimeProviderSettingsRejection::InvalidPath { .. }
            )
        ));
    }

    #[test]
    fn enabled_plan_toggles_runtime_without_touching_other_settings() {
        let current = snapshot(vec![codex_instance("codex", "Codex CLI")]);

        let CLIRuntimeProviderSettingsPlan::Send(plan) =
            plan_cli_runtime_provider_enabled_update(Some(&current), "codex", false)
        else {
            panic!("expected send plan");
        };

        assert!(!plan.snapshot.cli_runtimes.instances[0].enabled);
        assert!(
            !plan
                .update
                .cli_runtimes
                .as_ref()
                .expect("update should include CLI runtimes")
                .instances[0]
                .enabled
        );
    }
}
