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
            "Prefer facts stated by the user. Use assistant text only as supporting context for user/project facts.\n",
            "Do not extract the assistant's own self-description, identity, model/company, tone, or capabilities from assistant text unless the user explicitly asked to store that fact.\n",
            "Write fact content and value in the primary language of the source user text. If the turn is multilingual, use the language of the evidence quote for that fact.\n",
            "Do not translate names, quoted phrases, code identifiers, file paths, product names, technical terms, or user-provided labels.\n",
            "Keep evidence.quote_or_span as an exact source quote.\n",
            "Do not extract one-off commands, temporary debugging state, raw logs, guesses, secrets, passwords, API keys, tokens, credentials, or transient plans.\n",
            "Do not extract sensitive regulated health, legal, or financial facts unless the user explicitly asked to remember them.\n",
            "Do not generate canonical memory keys. Canonical keys are owned by the memory service.\n",
            "For each fact, propose ontology fields only. You propose; the memory service validates; the quality gate decides final storage/routing/rejection; the ownership router determines destination.\n",
            "Return no facts when evidence is weak, evidence.quote_or_span is missing, source actor is unclear, ownership is unclear, the fact is only an assistant inference about the user, or the content has no future-useful memory value.\n",
            "Do not decide whether a fact becomes active, pending, routed elsewhere, or rejected. Final write state is owned by the memory service.\n",
            "Set semantic fields carefully. The memory service computes final confidence and importance from semantic fields; numeric confidence and importance are non-authoritative and may be null.\n",
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
            "Allowed ontology.fact_class values: user_identity, user_biography, user_relationship, stable_user_preference, communication_preference, recurring_user_instruction, project_policy, project_decision, project_procedure, project_constraint, task_lifecycle_state, operational_observation, thread_local_state, tool_result_fact, assistant_self_description, generated_summary_fact, domain_owned_state, secret_or_credential, regulated_sensitive_fact, unknown.\n",
            "Allowed ontology.lifetime_class values: long_lived, project_lifetime, task_lifetime, thread_lifetime, session_only, naturally_expiring, instantaneous, unknown.\n",
            "Allowed ontology.evidence_class values: direct_user_assertion, user_correction, user_approval, assistant_inference, tool_observation, task_runtime_observation, system_observation, generated_summary, missing_or_weak.\n",
            "Allowed ontology.proposed_ownership_class values: durable_user_memory, durable_workspace_memory, durable_agent_memory, thread_episodic_context, task_runtime_state, domain_runtime_state, audit_only, reject.\n",
            "Ontology field meanings: fact_class is the semantic type of fact; lifetime_class is expected useful lifetime; evidence_class is why the source is or is not authoritative; proposed_ownership_class is where the fact appears to belong.\n\n",
            "JSON schema shape:\n",
            "{{\"facts\":[{{\"semantic\":{{\"intent\":\"explicit_store|implicit_candidate\",\"explicitness\":\"explicit|implicit|unclear\",\"category\":\"identity|preference|biography|relationship|recurring_instruction|project_policy|project_fact|project_decision|procedure|todo|constraint|communication_style|custom\",\"subject\":\"current_user|current_agent|workspace|project|person|organization|artifact|custom\",\"attribute\":\"name|birthday|preferred_language|communication_style|migration_policy|review_style|phase_naming|custom\",\"subject_key\":null,\"custom_subject\":null,\"custom_attribute\":null,\"scope_hint\":\"user_global|user_workspace|agent_global|agent_workspace|project_workspace|unknown\",\"durability\":\"long_lived|project_lifetime|session_only|transient|unknown\",\"sensitivity\":\"none|low|personal|regulated|secret|unknown\",\"certainty\":\"high|medium|low\"}},\"ontology\":{{\"fact_class\":\"typed fact class\",\"lifetime_class\":\"typed lifetime class\",\"evidence_class\":\"typed evidence class\",\"proposed_ownership_class\":\"typed ownership class\"}},\"content\":\"compact normalized memory sentence\",\"value\":\"optional normalized value or null\",\"evidence\":{{\"source_thread_id\":null,\"source_turn_id\":null,\"source_item_id\":null,\"source_ref\":\"turn.post_turn:user|turn.post_turn:assistant|turn.post_turn:tool\",\"quote_or_span\":\"short exact source quote\",\"extractor_reason\":\"short reason\"}},\"confidence\":null,\"importance\":null}}]}}\n\n",
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
        assert!(prompt.contains("You propose; the memory service validates"));
        assert!(prompt.contains("quality gate decides final storage/routing/rejection"));
        assert!(prompt.contains("ownership router determines destination"));
        assert!(prompt.contains("Return no facts when evidence is weak"));
        assert!(prompt.contains("ownership is unclear"));
        assert!(prompt.contains("Prefer facts stated by the user"));
        assert!(prompt.contains("primary language of the source user text"));
        assert!(prompt.contains("Keep evidence.quote_or_span as an exact source quote"));
        assert!(prompt.contains("numeric confidence and importance are non-authoritative"));
        assert!(prompt.contains("ontology.fact_class"));
        assert!(prompt.contains("ontology.lifetime_class"));
        assert!(prompt.contains("ontology.evidence_class"));
        assert!(prompt.contains("ontology.proposed_ownership_class"));
        assert!(prompt.contains("explicit_store"));
        assert!(prompt.contains("implicit_candidate"));
        assert!(!prompt.contains("Available memory tools"));
        assert!(!prompt.contains("memory_search"));
    }
}
