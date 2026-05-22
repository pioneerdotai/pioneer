#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveRecallProviderOutputPath {
    PreflightMemoryActiveRecall,
}

impl MemoryActiveRecallProviderOutputPath {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PreflightMemoryActiveRecall => "memory.activeRecall",
        }
    }
}

pub struct MemoryActiveRecallProviderOutputContractInput {
    pub output_path: MemoryActiveRecallProviderOutputPath,
    pub max_output_chars: Option<usize>,
    pub include_host_owned_field_rule: bool,
}

impl MemoryActiveRecallProviderOutputContractInput {
    pub const fn nested_preflight() -> Self {
        Self {
            output_path: MemoryActiveRecallProviderOutputPath::PreflightMemoryActiveRecall,
            max_output_chars: None,
            include_host_owned_field_rule: false,
        }
    }
}

pub fn render_memory_active_recall_provider_output_contract(
    input: &MemoryActiveRecallProviderOutputContractInput,
) -> String {
    let output_path = input.output_path.as_str();
    let mut contract = format!(
        concat!(
            "Active recall output contract for {output_path}:\n",
            "- `status`: one of `skip`, `run`, `uncertain`.\n",
            "- `reasonCode`: one of `provider_skip`, `provider_run`, `provider_uncertain`, `memory_likely`, `deterministic_sufficient`.\n",
            "- `confidence`: number from 0 to 1.\n",
            "- `modes`: array containing only values from structured input `availableModes`.\n",
            "- `targets`: array of advisory target objects; use [] when no exact target fields are structurally clear.\n",
            "- `diagnostics`: array of short strings.\n\n",
            "Allowed active recall modes:\n",
            "- `profile`, `project`, `durable`, `current_thread`, `related_thread`, `current_task`, `completed_task`, `thread_episodic`, `task_context`, `exact_canonical`.\n\n",
            "Allowed active recall target fields:\n",
            "- `scopeKind`: `user`, `workspace`, `thread`, `agent`, or `task`.\n",
            "- `factClass`: `user_identity`, `user_biography`, `user_relationship`, `stable_user_preference`, `communication_preference`, `recurring_user_instruction`, `project_policy`, `project_decision`, `project_procedure`, `project_constraint`, `task_lifecycle_state`, `operational_observation`, `thread_local_state`, `tool_result_fact`, `assistant_self_description`, `generated_summary_fact`, `domain_owned_state`, `secret_or_credential`, or `regulated_sensitive_fact`.\n",
            "- `category`: `identity`, `preference`, `biography`, `relationship`, `recurring_instruction`, `project_policy`, `project_fact`, `project_decision`, `procedure`, `todo`, `constraint`, `communication_style`, or `custom`.\n",
            "- `subject`: `current_user`, `current_agent`, `workspace`, `project`, `person`, `organization`, `artifact`, or `custom`.\n",
            "- `attribute`: `name`, `birthday`, `preferred_language`, `communication_style`, `migration_policy`, `review_style`, `phase_naming`, or `custom`.\n",
            "- `canonicalKey`: string or null.\n\n",
            "Active recall rules:\n",
            "- Return recall strategy only, not remembered facts and not a user-facing answer.\n",
            "- Use `skip` when the turn is self-contained or memory is not useful.\n",
            "- Use `run` when additional memory is likely to improve correctness, continuity, personalization, or consistency.\n",
            "- Use `uncertain` when the structured input is insufficient to choose safely.\n",
            "- For `run`, `modes` must contain at least one allowed mode.\n",
            "- For `skip` or `uncertain`, `modes` must be [] and `targets` must be [].\n",
            "- Use target fields exactly as listed; never output free-form strings for enum fields.\n",
            "- Do not use category names such as `identity` as `factClass`; use `user_identity` for the current user's name or identity facts.\n",
            "- Do not use category names such as `preference` as `attribute`; use `category=\"preference\"` and omit `attribute`, or use `attribute=\"custom\"` only for a clearly custom preference attribute.\n",
            "- Do not include `exact_canonical` unless an exact canonical target is present.\n",
            "- Do not include `current_task` or `task_context` unless current task context capability is available.\n",
            "- Do not include `completed_task` unless completed task summary capability is available.\n",
            "- Do not include `current_thread` or `thread_episodic` unless current thread episodic capability is available.\n",
            "- Do not include `related_thread` unless related thread search capability is available.\n",
            "- Keep diagnostics short and operational.",
        ),
        output_path = output_path
    );

    if input.include_host_owned_field_rule {
        contract.push_str("\n- Do not output host-owned active recall fields: `source`, `fallbackReason`, `debugFallback`, `providerUsed`, `providerFallbackUsed`, `providerInputChars`, `providerOutputChars`, `providerCall`, `provider`, `model`, `attempt`, `inputChars`, `outputChars`, or `elapsedMs`.");
    }

    if let Some(max_output_chars) = input.max_output_chars {
        let max_output_chars = max_output_chars.max(1);
        contract.push_str(
            format!("\n- Output must be no more than {max_output_chars} characters.").as_str(),
        );
    }

    contract
}

pub fn render_memory_active_recall_provider_output_example() -> &'static str {
    concat!(
        "{\n",
        "  \"status\": \"run\",\n",
        "  \"reasonCode\": \"memory_likely\",\n",
        "  \"confidence\": 0.86,\n",
        "  \"modes\": [\"profile\"],\n",
        "  \"targets\": [\n",
        "    {\n",
        "      \"scopeKind\": \"user\",\n",
        "      \"factClass\": \"user_identity\",\n",
        "      \"category\": \"identity\",\n",
        "      \"subject\": \"current_user\",\n",
        "      \"attribute\": \"name\",\n",
        "      \"canonicalKey\": null\n",
        "    }\n",
        "  ],\n",
        "  \"diagnostics\": [\"identity_lookup\"]\n",
        "}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_recall_provider_output_contract_is_strict_strategy_only_contract() {
        let contract = render_memory_active_recall_provider_output_contract(
            &MemoryActiveRecallProviderOutputContractInput::nested_preflight(),
        );

        assert!(contract.contains("Active recall output contract for memory.activeRecall"));
        assert!(contract.contains("Return recall strategy only"));
        assert!(contract.contains("`factClass`: `user_identity`"));
        assert!(contract.contains("`subject`: `current_user`"));
        assert!(contract.contains("`attribute`: `name`"));
        assert!(contract.contains("Do not use category names such as `identity` as `factClass`"));
        assert!(contract.contains("never output free-form strings for enum fields"));
        assert!(contract.contains("For `run`, `modes` must contain at least one allowed mode."));
        assert!(contract.contains("For `skip` or `uncertain`, `modes` must be []"));
        assert!(!contract.contains(" | string"));
        assert!(!contract.contains("optional string"));
    }

    #[test]
    fn active_recall_provider_output_example_is_valid_json() {
        serde_json::from_str::<serde_json::Value>(
            render_memory_active_recall_provider_output_example(),
        )
        .expect("active recall output example must be valid JSON");
    }
}
