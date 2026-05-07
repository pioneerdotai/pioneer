#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPostTurnExtractorPromptInput {
    pub user_text: String,
    pub assistant_text: String,
    pub tool_events_summary: String,
    pub domain_events_summary: String,
    pub memory_manifest: String,
    pub max_facts: usize,
}

pub fn render_memory_post_turn_extractor_prompt(
    input: &MemoryPostTurnExtractorPromptInput,
) -> String {
    let max_facts = input.max_facts.max(1);
    format!(
        concat!(
            "You are Pioneer post-turn memory extractor.\n",
            "You are not the main assistant. Do not answer the user. Do not give advice. Do not call tools.\n",
            "Inspect only the provided completed turn and memory manifest.\n",
            "Extract only durable semantic facts that are useful for future turns.\n",
            "Do not extract one-off commands, temporary debugging state, raw logs, guesses, secrets, passwords, API keys, tokens, credentials, or transient plans.\n",
            "Do not extract sensitive regulated health, legal, or financial facts unless the user explicitly asked to remember them.\n",
            "Do not generate canonical memory keys. Canonical keys are owned by the memory service.\n",
            "Do not decide whether a fact becomes active, pending, or rejected. Final write state is owned by the memory service.\n",
            "Return strict JSON only. No markdown. No prose outside JSON.\n",
            "Return at most {max_facts} facts.\n\n",
            "Allowed semantic.intent values for facts: explicit_store, implicit_candidate.\n",
            "Allowed semantic.explicitness values: explicit, implicit, unclear.\n",
            "Allowed semantic.category values: identity, preference, biography, relationship, recurring_instruction, project_policy, project_fact, project_decision, procedure, todo, constraint, communication_style, custom.\n",
            "Allowed semantic.subject values: current_user, current_agent, workspace, project, person, organization, artifact, custom.\n",
            "Allowed semantic.attribute values: name, birthday, preferred_language, communication_style, migration_policy, review_style, phase_naming, custom.\n",
            "Allowed semantic.scope_hint values: user_global, user_workspace, agent_global, agent_workspace, project_workspace, unknown.\n",
            "Allowed semantic.durability values: long_lived, project_lifetime, session_only, transient, unknown.\n",
            "Allowed semantic.sensitivity values: none, low, personal, regulated, secret, unknown.\n",
            "Allowed semantic.certainty values: high, medium, low.\n\n",
            "JSON schema shape:\n",
            "{{\"facts\":[{{\"semantic\":{{\"intent\":\"explicit_store|implicit_candidate\",\"explicitness\":\"explicit|implicit|unclear\",\"category\":\"identity|preference|biography|relationship|recurring_instruction|project_policy|project_fact|project_decision|procedure|todo|constraint|communication_style|custom\",\"subject\":\"current_user|current_agent|workspace|project|person|organization|artifact|custom\",\"attribute\":\"name|birthday|preferred_language|communication_style|migration_policy|review_style|phase_naming|custom\",\"subject_key\":null,\"custom_subject\":null,\"custom_attribute\":null,\"scope_hint\":\"user_global|user_workspace|agent_global|agent_workspace|project_workspace|unknown\",\"durability\":\"long_lived|project_lifetime|session_only|transient|unknown\",\"sensitivity\":\"none|low|personal|regulated|secret|unknown\",\"certainty\":\"high|medium|low\"}},\"content\":\"compact normalized memory sentence\",\"value\":\"optional normalized value or null\",\"evidence\":{{\"source_thread_id\":null,\"source_turn_id\":null,\"source_item_id\":null,\"source_ref\":\"turn.post_turn:user|turn.post_turn:assistant|turn.post_turn:tool\",\"quote_or_span\":\"short exact source quote\",\"extractor_reason\":\"short reason\"}},\"confidence\":0.0,\"importance\":0.0}}]}}\n\n",
            "Memory manifest:\n{memory_manifest}\n\n",
            "User text:\n{user_text}\n\n",
            "Assistant text:\n{assistant_text}\n\n",
            "Tool events:\n{tool_events_summary}\n\n",
            "Domain events:\n{domain_events_summary}\n"
        ),
        max_facts = max_facts,
        memory_manifest = bounded_section(&input.memory_manifest),
        user_text = bounded_section(&input.user_text),
        assistant_text = bounded_section(&input.assistant_text),
        tool_events_summary = bounded_section(&input.tool_events_summary),
        domain_events_summary = bounded_section(&input.domain_events_summary),
    )
}

fn bounded_section(value: &str) -> String {
    let normalized = value.trim();
    if normalized.is_empty() {
        "(empty)".to_owned()
    } else {
        normalized.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn post_turn_extractor_prompt_is_strict_and_not_agent_prompt() {
        let prompt =
            render_memory_post_turn_extractor_prompt(&MemoryPostTurnExtractorPromptInput {
                user_text: "Меня зовут Александр".to_owned(),
                assistant_text: "Запомню.".to_owned(),
                tool_events_summary: String::new(),
                domain_events_summary: String::new(),
                memory_manifest: "- active: none".to_owned(),
                max_facts: 4,
            });

        assert!(prompt.contains("post-turn memory extractor"));
        assert!(prompt.contains("Do not answer the user"));
        assert!(prompt.contains("Return strict JSON only"));
        assert!(prompt.contains("Do not generate canonical memory keys"));
        assert!(prompt.contains("Final write state is owned by the memory service"));
        assert!(prompt.contains("explicit_store"));
        assert!(prompt.contains("implicit_candidate"));
        assert!(!prompt.contains("Available memory tools"));
        assert!(!prompt.contains("memory_search"));
    }
}
