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
    pub sections: MemoryActiveRecallProviderOutputSections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryActiveRecallProviderOutputSections {
    pub durable: bool,
    pub episodic: bool,
}

impl MemoryActiveRecallProviderOutputSections {
    pub const fn all() -> Self {
        Self {
            durable: true,
            episodic: true,
        }
    }
}

impl MemoryActiveRecallProviderOutputContractInput {
    pub const fn nested_preflight() -> Self {
        Self {
            output_path: MemoryActiveRecallProviderOutputPath::PreflightMemoryActiveRecall,
            max_output_chars: None,
            include_host_owned_field_rule: false,
            sections: MemoryActiveRecallProviderOutputSections::all(),
        }
    }
}

pub fn render_memory_active_recall_provider_output_contract(
    input: &MemoryActiveRecallProviderOutputContractInput,
) -> String {
    let output_path = input.output_path.as_str();

    let envelope_fields = match (input.sections.durable, input.sections.episodic) {
        (true, true) => "`durable`, `episodic`, and `diagnostics`",
        (true, false) => "`durable` and `diagnostics`",
        (false, true) => "`episodic` and `diagnostics`",
        (false, false) => "`diagnostics`",
    };

    let mut contract = format!(
        concat!(
            "Active recall output contract for {output_path}:\n",
            "\n",
            "Purpose:\n",
            "- Return recall strategy only. Do not return remembered facts, retrieved context, or a user-facing answer.\n",
        ),
        output_path = output_path,
    );

    if input.sections.durable {
        contract.push_str("- Durable recall plans long-lived memory lookup.\n");
    }

    if input.sections.episodic {
        contract.push_str("- Episodic recall plans thread/task history lookup.\n");
    }

    contract.push_str(
        format!(
            concat!(
            "- The host validates this plan before executing any read.\n",
            "\n",
            "Top-level envelope:\n",
                "- Return an envelope object with exactly these top-level fields: {envelope_fields}.\n",
            "- `diagnostics`: array of short operational strings about the overall planning decision.\n",
            "\n",
            "Subplan status fields:\n",
            "- `status`: one of `skip`, `run`, `uncertain`.\n",
            "- `reasonCode`: one of `provider_skip`, `provider_run`, `provider_uncertain`, `memory_likely`.\n",
            "- `confidence`: number from 0 to 1.\n",
            "- Use `skip` when that memory domain is self-contained or not useful for this turn.\n",
            "- Use `run` when that memory domain is likely to improve correctness, continuity, personalization, or consistency.\n",
            "- Use `uncertain` when structured input is insufficient to choose safely.\n",
            "\n",
            ),
            envelope_fields = envelope_fields,
        )
        .as_str(),
    );

    if input.sections.durable {
        contract.push_str(concat!(
            "Durable subplan schema:\n",
            "- `durable.status`: one of `skip`, `run`, `uncertain`.\n",
            "- `durable.reasonCode`: one of `provider_skip`, `provider_run`, `provider_uncertain`, `memory_likely`.\n",
            "- `durable.confidence`: number from 0 to 1.\n",
            "- `durable.modes`: array containing only durable modes from structured input `availableDurableModes`.\n",
            "- `durable.targets`: array of advisory target objects; use [] when no exact target fields are structurally clear.\n",
            "- Durable modes are only `profile`, `project`, `durable`, and `exact_canonical`.\n",
            "- Durable recall is for long-lived facts, user profile, project decisions, stable preferences, and exact canonical memory keys.\n",
            "- For durable exact targets and canonical keys, preserve concrete user-facing entities and identifiers from structured input; do not invent translated or generic names.\n",
            "\n",
            "Durable subplan validation:\n",
            "- For `durable.status=run`, `durable.modes` must contain at least one allowed durable mode.\n",
            "- For `durable.status=skip` or `uncertain`, `durable.modes` must be [] and `durable.targets` must be [].\n",
            "- Do not include durable mode `exact_canonical` unless an exact canonical target is present.\n",
            "\n",
        ));
    }

    if input.sections.episodic {
        contract.push_str(concat!(
            "Episodic subplan schema:\n",
            "- `episodic.status`: one of `skip`, `run`, `uncertain`.\n",
            "- `episodic.reasonCode`: one of `provider_skip`, `provider_run`, `provider_uncertain`, `memory_likely`.\n",
            "- `episodic.confidence`: number from 0 to 1.\n",
            "- `episodic.queries`: array of episodic search query objects.\n\n",
            "Episodic query object fields:\n",
            "- `mode`: one allowed episodic query mode from structured input `availableEpisodicModes`.\n",
            "- `query`: compact search text for thread/task context recall, not a remembered fact and not the final answer.\n",
            "- `targets`: advisory target objects; use [] when no target fields are structurally clear.\n",
            "- `topK`: optional positive integer.\n",
            "- `maxChars`: optional positive integer.\n",
            "\n",
            "Episodic query modes:\n",
            "- `current_thread`, `related_thread`, `workspace_thread`, `current_task`, `completed_task`, `thread_episodic`, `task_context`.\n",
            "- Episodic recall is for current-thread, related-thread, workspace-thread, current-task, or completed-task context.\n",
            "\n",
            "Episodic query construction:\n",
            "- Build each query from the current user input and structured non-transcript fields only.\n",
            "- Every `query` must be written in the same natural language and script as the current user input.\n",
            "- The provider default language, system prompt language, internal reasoning language, and English technical labels must never determine query language.\n",
            "- If the current user input is multilingual, preserve the user's languages as written instead of normalizing everything to one language.\n",
            "- Preserve concrete names, places, dates, objects, project names, topics, and domain words exactly as they appear in the current input.\n",
            "- Do not translate, romanize, transliterate, summarize into English, or replace concrete user terms with labels from another language.\n",
            "- If a useful same-language query cannot be formed directly from the current input and structured non-transcript fields, do not guess missing context; use `episodic.status=uncertain`.\n",
            "- If episodic recall is clearly not useful for this turn, use `episodic.status=skip`.\n",
            "- Never output a vague, generic, or invented query just to satisfy `episodic.status=run`.\n",
            "- Do not output meta-descriptions such as `previous turn context`, `request context`, `weather request context`, or `conversation context`.\n",
            "\n",
            "Episodic subplan validation:\n",
            "- For `episodic.status=run`, `episodic.queries` must contain at least one bounded search query object.\n",
            "- For `episodic.status=skip` or `uncertain`, `episodic.queries` must be [].\n",
            "- Do not include episodic query mode `current_task` or `task_context` unless current task context capability is available.\n",
            "- Do not include episodic query mode `completed_task` unless completed task summary capability is available.\n",
            "- Do not include episodic query mode `current_thread` or `thread_episodic` unless structured input `threadEpisodic.currentThreadRecallAvailable` is true.\n",
            "- Do not include episodic query mode `related_thread` unless related thread search capability is available.\n",
            "- Do not include episodic query mode `workspace_thread` unless workspace thread search capability is available and workspace context is present.\n",
            "\n",
        ));
    }

    if input.sections.durable || input.sections.episodic {
        contract.push_str(concat!(
            "Target object fields:\n",
            "- `scopeKind`: `user`, `workspace`, `thread`, `agent`, or `task`.\n",
            "- `factClass`: `user_identity`, `user_biography`, `user_relationship`, `stable_user_preference`, `communication_preference`, `recurring_user_instruction`, `project_policy`, `project_decision`, `project_procedure`, `project_constraint`, `task_lifecycle_state`, `operational_observation`, `thread_local_state`, `tool_result_fact`, `assistant_self_description`, `generated_summary_fact`, `domain_owned_state`, `secret_or_credential`, or `regulated_sensitive_fact`.\n",
            "- `category`: `identity`, `preference`, `biography`, `relationship`, `recurring_instruction`, `project_policy`, `project_fact`, `project_decision`, `procedure`, `todo`, `constraint`, `communication_style`, or `custom`.\n",
            "- `subject`: `current_user`, `current_agent`, `workspace`, `project`, `person`, `organization`, `artifact`, or `custom`.\n",
            "- `attribute`: `name`, `birthday`, `preferred_language`, `communication_style`, `migration_policy`, `review_style`, `phase_naming`, or `custom`.\n",
            "- `canonicalKey`: string or null.\n\n",
        ));
    }

    if input.sections.durable && input.sections.episodic {
        contract.push_str(concat!(
            "Cross-domain validation:\n",
            "- Do not place episodic modes in `durable.modes`; use `episodic.queries` instead.\n",
            "- Do not place durable modes in `episodic.queries[].mode`; use `durable.modes` instead.\n",
        ));
    } else {
        contract.push_str("Output validation:\n");
    }

    contract.push_str(concat!(
            "- Use target fields exactly as listed; never output free-form strings for enum fields.\n",
            "- Do not use category names such as `identity` as `factClass`; use `user_identity` for the current user's name or identity facts.\n",
            "- Do not use category names such as `preference` as `attribute`; use `category=\"preference\"` and omit `attribute`, or use `attribute=\"custom\"` only for a clearly custom preference attribute.\n",
            "- Keep diagnostics short and operational.",
    ));

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
        "  \"durable\": {\n",
        "    \"status\": \"run\",\n",
        "    \"reasonCode\": \"memory_likely\",\n",
        "    \"confidence\": 0.86,\n",
        "    \"modes\": [\"profile\"],\n",
        "    \"targets\": [\n",
        "      {\n",
        "        \"scopeKind\": \"user\",\n",
        "        \"factClass\": \"user_identity\",\n",
        "        \"category\": \"identity\",\n",
        "        \"subject\": \"current_user\",\n",
        "        \"attribute\": \"name\",\n",
        "        \"canonicalKey\": null\n",
        "      }\n",
        "    ]\n",
        "  },\n",
        "  \"episodic\": {\n",
        "    \"status\": \"skip\",\n",
        "    \"reasonCode\": \"provider_skip\",\n",
        "    \"confidence\": 1.0,\n",
        "    \"queries\": []\n",
        "  },\n",
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
        assert!(contract.contains("`workspace_thread`"));
        assert!(contract.contains("`factClass`: `user_identity`"));
        assert!(contract.contains("`subject`: `current_user`"));
        assert!(contract.contains("`attribute`: `name`"));
        assert!(contract.contains("Do not use category names such as `identity` as `factClass`"));
        assert!(contract.contains("never output free-form strings for enum fields"));
        assert!(contract.contains("Return an envelope object"));
        assert!(contract.contains("Purpose:"));
        assert!(contract.contains("Durable subplan schema:"));
        assert!(contract.contains("Episodic subplan schema:"));
        assert!(contract.contains("Episodic query construction:"));
        assert!(contract.contains("same natural language and script as the current user input"));
        assert!(contract.contains("provider default language"));
        assert!(contract.contains("current user input is multilingual"));
        assert!(contract.contains("current user input and structured non-transcript fields only"));
        assert!(contract.contains("do not guess missing context"));
        assert!(contract.contains("Never output a vague, generic, or invented query"));
        assert!(contract.contains("Do not translate, romanize, transliterate"));
        assert!(contract.contains(
            "`query`: compact search text for thread/task context recall, not a remembered fact and not the final answer"
        ));
        assert!(!contract.contains("fullInputQuery"));
        assert!(!contract.contains("Bad query:"));
        assert!(!contract.contains("Good query"));
        assert!(!contract.contains("recentThreadContext"));
        assert!(contract.contains("Do not place episodic modes in `durable.modes`"));
        assert!(
            contract
                .contains("For `durable.status=run`, `durable.modes` must contain at least one")
        );
        assert!(contract.contains("For `episodic.status=run`, `episodic.queries` must contain"));
        assert!(contract.contains(
            "Do not include episodic query mode `workspace_thread` unless workspace thread search capability is available"
        ));
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
