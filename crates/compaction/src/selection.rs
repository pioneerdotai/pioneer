use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Api,
    Codex,
    Claude,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelSelection {
    pub transport: Transport,
    pub instance: String,
    pub model: String,
    pub effort: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompactionSettings {
    #[serde(default)]
    pub selection: Option<ModelSelection>,
}

/// Resolve the entire selection; never borrow the effort or instance of a fallback.
pub fn effective_selection<'a>(
    current: &'a ModelSelection,
    general: Option<&'a ModelSelection>,
    cli_override: Option<&'a ModelSelection>,
) -> &'a ModelSelection {
    match current.transport {
        Transport::Api => general.unwrap_or(current),
        _ => cli_override.or(general).unwrap_or(current),
    }
}

/// Admission captures settings. Disabling future admission does not cancel this snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationAdmission {
    pub selection: ModelSelection,
    pub admitted_at_ms: u64,
    pub deadline_ms: u64,
}
impl CompactionSettings {
    pub fn admit(
        &self,
        current: &ModelSelection,
        cli_override: Option<&ModelSelection>,
        now_ms: u64,
    ) -> anyhow::Result<OperationAdmission> {
        let selection = effective_selection(current, self.selection.as_ref(), cli_override).clone();
        anyhow::ensure!(
            !selection.instance.is_empty() && !selection.model.is_empty(),
            "invalid explicit compaction selection"
        );
        Ok(OperationAdmission {
            selection,
            admitted_at_ms: now_ms,
            deadline_ms: now_ms.saturating_add(crate::OPERATION_MILLIS),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn model(transport: Transport, name: &str) -> ModelSelection {
        ModelSelection {
            transport,
            instance: name.into(),
            model: name.into(),
            effort: None,
        }
    }
    #[test]
    fn transport_is_selected_independently_of_working_turn() {
        let api = model(Transport::Api, "api");
        let cli = model(Transport::Codex, "cli");
        let override_model = model(Transport::Claude, "override");
        assert_eq!(
            effective_selection(&api, Some(&cli), Some(&override_model)),
            &cli
        );
        assert_eq!(effective_selection(&cli, Some(&api), None), &api);
        assert_eq!(
            effective_selection(&cli, Some(&api), Some(&override_model)),
            &override_model
        );
        assert_eq!(effective_selection(&cli, None, None), &cli);
    }
    #[test]
    fn admission_uses_current_selection_and_keeps_existing_snapshot() {
        let current = model(Transport::Api, "current");
        let mut settings = CompactionSettings::default();
        let admitted = settings.admit(&current, None, 100).unwrap();
        settings.selection = Some(model(Transport::Claude, "summary"));
        let next = settings.admit(&current, None, 200).unwrap();
        assert_eq!(admitted.selection, current);
        assert_eq!(next.selection, settings.selection.clone().unwrap());
        assert_eq!(admitted.deadline_ms, 100 + crate::OPERATION_MILLIS);
        assert_eq!(next.deadline_ms, 200 + crate::OPERATION_MILLIS);
    }
}
