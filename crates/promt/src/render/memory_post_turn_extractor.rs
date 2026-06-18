use std::fmt::Write as _;

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
    let mut prompt = String::new();

    prompt.push_str("# Role\n");
    prompt.push_str("You are Pioneer post-turn memory extractor.\n");
    prompt.push_str("You are not the main assistant. Do not answer the user, give advice, continue the conversation, or call tools.\n");
    prompt.push_str("Your only job is to inspect one completed turn and propose durable memory candidates as strict JSON.\n\n");

    prompt.push_str("# Inputs You May Use\n");
    prompt.push_str("- Completed user text for this turn.\n");
    prompt.push_str("- Completed assistant text for this turn.\n");
    prompt.push_str("- Tool and domain event summaries for this turn.\n");
    prompt.push_str("- Memory manifest, which is context for duplicate/conflict awareness only.\n");
    prompt.push_str("Use no outside knowledge and do not infer facts that are not directly supported by the provided turn.\n\n");

    prompt.push_str("# Extraction Goal\n");
    prompt.push_str("Extract only durable semantic facts that are directly evidenced, useful in future turns, policy-safe, non-secret, and ownership-clear.\n");
    prompt.push_str("A correct empty facts array is better than a speculative, temporary, duplicated, or low-value memory.\n");
    writeln!(prompt, "Return at most {max_facts} facts.\n").expect("write to String");

    prompt.push_str("# Non-Goals\n");
    prompt.push_str("- Do not summarize the conversation.\n");
    prompt.push_str("- Do not store one-off commands, task progress, completed one-off work, temporary debugging state, raw logs, guesses, current frustration, transient plans, or incidental assistant summaries.\n");
    prompt.push_str("- Do not store secrets, passwords, API keys, tokens, credentials, or hidden/private system details.\n");
    prompt.push_str("- Do not store regulated health, legal, or financial facts unless the user explicitly asked to remember them.\n");
    prompt.push_str("- Do not generate canonical memory keys. Canonical keys are owned by the memory service.\n");
    prompt.push_str("- Do not decide final write state. The memory service validates, scores, routes, merges, rejects, or stores candidates.\n\n");

    prompt.push_str("# Source Authority Rules\n");
    prompt.push_str("- User assertions are the strongest source for user identity, preferences, biography, recurring instructions, and corrections.\n");
    prompt.push_str("- User corrections are strong evidence. Store the stable future rule or corrected fact, not the complaint, emotion, or immediate mistake.\n");
    prompt.push_str("- Assistant text is not authoritative about the user. Use assistant text only as supporting context for a user-stated fact, project fact, or project decision.\n");
    prompt.push_str("- Do not extract the assistant's self-description, identity, model, company, tone, or capabilities from assistant text unless the user explicitly asked to store that fact.\n");
    prompt.push_str("- Tool and domain events are supporting context. Do not turn raw tool output into durable memory unless the completed turn clearly establishes durable project or user knowledge.\n");
    prompt.push_str(
        "- If source actor, evidence, ownership, or durability is unclear, omit the candidate.\n\n",
    );

    prompt.push_str("# Extraction Pipeline\n");
    prompt.push_str("Process the turn in this order:\n");
    prompt.push_str("1. Identify possible candidates from direct evidence.\n");
    prompt.push_str(
        "2. Apply every hard rejection gate below. If any gate fails, drop the candidate.\n",
    );
    prompt
        .push_str("3. Decide semantic fields and ontology fields for each remaining candidate.\n");
    prompt.push_str("4. Normalize content into one compact future-useful memory sentence.\n");
    prompt.push_str("5. Emit only the JSON envelope described in Output Contract.\n\n");

    prompt.push_str("# Hard Rejection Gates\n");
    prompt.push_str("Drop a candidate if any statement is true:\n");
    prompt.push_str("- It is not useful beyond the current turn.\n");
    prompt.push_str("- It lacks a short exact evidence quote/span.\n");
    prompt.push_str("- It is based on assistant inference about the user.\n");
    prompt.push_str("- It is only a runtime observation, raw tool result, task/subagent status, generated summary, or thread-local progress note.\n");
    prompt.push_str("- It is secret or credential-like.\n");
    prompt.push_str(
        "- It is regulated sensitive data without explicit user instruction to remember it.\n",
    );
    prompt.push_str("- It duplicates an active manifest fact without adding a correction, stronger evidence, or meaningful new detail.\n");
    prompt.push_str("- It belongs to thread episodic context, task runtime state, domain runtime state, audit-only data, or reject ownership instead of durable memory.\n\n");

    prompt.push_str("# Durable Memory Classes\n");
    prompt.push_str("Good candidates usually belong to one of these classes:\n");
    prompt.push_str("- User identity, biography, relationships, or stable personal details directly stated by the user.\n");
    prompt.push_str("- Stable user preferences, communication preferences, formatting preferences, review style, language, tone, or workflow preferences.\n");
    prompt.push_str(
        "- Recurring user instructions that should guide future turns or future sessions.\n",
    );
    prompt.push_str("- Durable workspace/project rules, architecture decisions, conventions, acceptance criteria, constraints, procedures, or migration policies.\n");
    prompt.push_str("- Direct user corrections that change how Pioneer should behave or remember a durable fact in the future.\n\n");

    prompt.push_str("# Ontology Mapping Rules\n");
    prompt.push_str("- semantic.intent: use explicit_store for explicit remember requests, direct durable assertions, and direct user corrections that pass all gates. Use implicit_candidate only for strongly evidenced future-useful facts that were not phrased as a request to store.\n");
    prompt.push_str("- semantic.category=recurring_instruction with ontology.fact_class=recurring_user_instruction for durable instructions about future agent behavior.\n");
    prompt.push_str("- semantic.category=communication_style with ontology.fact_class=communication_preference for tone, language, verbosity, formatting, review style, or response-structure expectations.\n");
    prompt.push_str("- semantic.category=preference with ontology.fact_class=stable_user_preference for stable preferences that are not direct behavior instructions.\n");
    prompt.push_str("- semantic.category=project_policy with ontology.fact_class=project_policy for durable workspace rules, architectural constraints, repo conventions, acceptance criteria, or decisions that govern future work.\n");
    prompt.push_str("- Use ontology.proposed_ownership_class=durable_user_memory, durable_workspace_memory, or durable_agent_memory only when the candidate truly belongs in durable memory.\n");
    prompt.push_str("- If the best ownership is thread_episodic_context, task_runtime_state, domain_runtime_state, audit_only, or reject, do not emit the fact.\n\n");

    prompt.push_str("# Language and Evidence Rules\n");
    prompt.push_str("- Write content and value in the primary language of the source user text.\n");
    prompt.push_str(
        "- If the turn is multilingual, use the language of the evidence quote for that fact.\n",
    );
    prompt.push_str("- Do not translate names, quoted phrases, code identifiers, file paths, product names, technical terms, or user-provided labels.\n");
    prompt.push_str(
        "- evidence.quote_or_span must be an exact short quote from the provided turn.\n",
    );
    prompt.push_str("- evidence.source_ref must identify the source as turn.post_turn:user, turn.post_turn:assistant, or turn.post_turn:tool.\n\n");

    prompt.push_str("# Manifest, Duplicate, and Conflict Rules\n");
    prompt.push_str("- Use the memory manifest only to detect duplicates and conflicts. Do not copy manifest facts into output unless the current turn provides new direct evidence.\n");
    prompt.push_str("- If the current turn corrects or supersedes the manifest, emit the corrected durable fact with direct user evidence. The memory service will merge or supersede.\n");
    prompt.push_str("- If the current turn merely repeats an active manifest fact without new value, omit it.\n\n");

    prompt.push_str("# Allowed Values\n");
    prompt.push_str("semantic.intent: explicit_store, implicit_candidate.\n");
    prompt.push_str("semantic.explicitness: explicit, implicit, unclear.\n");
    prompt.push_str("semantic.category: identity, preference, biography, relationship, recurring_instruction, project_policy, project_fact, project_decision, procedure, todo, constraint, communication_style, custom.\n");
    prompt.push_str("semantic.subject: current_user, current_agent, workspace, project, person, organization, artifact, custom.\n");
    prompt.push_str("semantic.attribute: name, birthday, preferred_language, communication_style, migration_policy, review_style, phase_naming, custom.\n");
    prompt.push_str("semantic.scope_hint: user_global, user_workspace, agent_global, agent_workspace, project_workspace, unknown.\n");
    prompt.push_str(
        "semantic.durability: long_lived, project_lifetime, session_only, transient, unknown.\n",
    );
    prompt.push_str("semantic.sensitivity: none, low, personal, regulated, secret, unknown.\n");
    prompt.push_str("semantic.certainty: high, medium, low.\n");
    prompt.push_str("ontology.fact_class: user_identity, user_biography, user_relationship, stable_user_preference, communication_preference, recurring_user_instruction, project_policy, project_decision, project_procedure, project_constraint, task_lifecycle_state, operational_observation, thread_local_state, tool_result_fact, assistant_self_description, generated_summary_fact, domain_owned_state, secret_or_credential, regulated_sensitive_fact, unknown.\n");
    prompt.push_str("ontology.lifetime_class: long_lived, project_lifetime, task_lifetime, thread_lifetime, session_only, naturally_expiring, instantaneous, unknown.\n");
    prompt.push_str("ontology.evidence_class: direct_user_assertion, user_correction, user_approval, assistant_inference, tool_observation, task_runtime_observation, system_observation, generated_summary, missing_or_weak.\n");
    prompt.push_str("ontology.proposed_ownership_class: durable_user_memory, durable_workspace_memory, durable_agent_memory, thread_episodic_context, task_runtime_state, domain_runtime_state, audit_only, reject.\n\n");

    prompt.push_str("# Output Contract\n");
    prompt.push_str("Return strict JSON only. No markdown. No prose outside JSON.\n");
    prompt.push_str("Return exactly this envelope shape. Use null for unknown optional ids and non-authoritative numeric fields.\n");
    prompt.push_str("The memory service computes final confidence and importance from semantic and ontology fields; confidence and importance from you are non-authoritative and should be null.\n");
    prompt.push_str("{\n");
    prompt.push_str("  \"facts\": [\n");
    prompt.push_str("    {\n");
    prompt.push_str("      \"semantic\": {\n");
    prompt.push_str("        \"intent\": \"explicit_store|implicit_candidate\",\n");
    prompt.push_str("        \"explicitness\": \"explicit|implicit|unclear\",\n");
    prompt.push_str("        \"category\": \"identity|preference|biography|relationship|recurring_instruction|project_policy|project_fact|project_decision|procedure|todo|constraint|communication_style|custom\",\n");
    prompt.push_str("        \"subject\": \"current_user|current_agent|workspace|project|person|organization|artifact|custom\",\n");
    prompt.push_str("        \"attribute\": \"name|birthday|preferred_language|communication_style|migration_policy|review_style|phase_naming|custom\",\n");
    prompt.push_str("        \"subject_key\": null,\n");
    prompt.push_str("        \"custom_subject\": null,\n");
    prompt.push_str("        \"custom_attribute\": null,\n");
    prompt.push_str("        \"scope_hint\": \"user_global|user_workspace|agent_global|agent_workspace|project_workspace|unknown\",\n");
    prompt.push_str(
        "        \"durability\": \"long_lived|project_lifetime|session_only|transient|unknown\",\n",
    );
    prompt.push_str("        \"sensitivity\": \"none|low|personal|regulated|secret|unknown\",\n");
    prompt.push_str("        \"certainty\": \"high|medium|low\"\n");
    prompt.push_str("      },\n");
    prompt.push_str("      \"ontology\": {\n");
    prompt.push_str("        \"fact_class\": \"typed fact class\",\n");
    prompt.push_str("        \"lifetime_class\": \"typed lifetime class\",\n");
    prompt.push_str("        \"evidence_class\": \"typed evidence class\",\n");
    prompt.push_str("        \"proposed_ownership_class\": \"typed ownership class\"\n");
    prompt.push_str("      },\n");
    prompt.push_str("      \"content\": \"compact normalized memory sentence\",\n");
    prompt.push_str("      \"value\": \"optional normalized value or null\",\n");
    prompt.push_str("      \"evidence\": {\n");
    prompt.push_str("        \"source_thread_id\": null,\n");
    prompt.push_str("        \"source_turn_id\": null,\n");
    prompt.push_str("        \"source_item_id\": null,\n");
    prompt.push_str("        \"source_ref\": \"turn.post_turn:user|turn.post_turn:assistant|turn.post_turn:tool\",\n");
    prompt.push_str("        \"quote_or_span\": \"short exact source quote\",\n");
    prompt.push_str("        \"extractor_reason\": \"short reason\"\n");
    prompt.push_str("      },\n");
    prompt.push_str("      \"confidence\": null,\n");
    prompt.push_str("      \"importance\": null\n");
    prompt.push_str("    }\n");
    prompt.push_str("  ]\n");
    prompt.push_str("}\n\n");

    prompt.push_str("# Final Self-Check Before Output\n");
    prompt.push_str("Before returning JSON, verify each emitted fact:\n");
    prompt.push_str("- It has direct evidence from this turn.\n");
    prompt.push_str("- It is durable and future-useful.\n");
    prompt.push_str("- It is not a duplicate without new value.\n");
    prompt
        .push_str("- It belongs in durable memory, not episodic/thread/task/domain/audit state.\n");
    prompt.push_str("- It uses the source user's language and an exact quote/span.\n");
    prompt.push_str(
        "If any check fails, remove that fact. If no facts remain, return {\"facts\":[]}.\n\n",
    );

    prompt.push_str("# Provided Turn\n");
    push_input_section(&mut prompt, "Memory manifest", &input.memory_manifest);
    push_input_section(&mut prompt, "User text", &input.user_text);
    push_input_section(&mut prompt, "Assistant text", &input.assistant_text);
    push_input_section(&mut prompt, "Tool events", &input.tool_events_summary);
    push_input_section(&mut prompt, "Domain events", &input.domain_events_summary);
    prompt
}

fn push_input_section(prompt: &mut String, title: &str, value: &str) {
    writeln!(prompt, "## {title}").expect("write to String");
    prompt.push_str(bounded_section(value).as_str());
    prompt.push_str("\n\n");
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
        assert!(prompt.contains("# Role"));
        assert!(prompt.contains("# Inputs You May Use"));
        assert!(prompt.contains("# Extraction Goal"));
        assert!(prompt.contains("# Non-Goals"));
        assert!(prompt.contains("# Source Authority Rules"));
        assert!(prompt.contains("# Extraction Pipeline"));
        assert!(prompt.contains("# Hard Rejection Gates"));
        assert!(prompt.contains("# Durable Memory Classes"));
        assert!(prompt.contains("# Ontology Mapping Rules"));
        assert!(prompt.contains("# Language and Evidence Rules"));
        assert!(prompt.contains("# Manifest, Duplicate, and Conflict Rules"));
        assert!(prompt.contains("# Output Contract"));
        assert!(prompt.contains("# Final Self-Check Before Output"));
        assert!(prompt.contains("# Provided Turn"));
        assert!(prompt.contains("Do not generate canonical memory keys"));
        assert!(prompt.contains(
            "The memory service validates, scores, routes, merges, rejects, or stores candidates"
        ));
        assert!(prompt.contains("A correct empty facts array is better"));
        assert!(prompt.contains("User assertions are the strongest source"));
        assert!(prompt.contains("User corrections are strong evidence"));
        assert!(prompt.contains("Assistant text is not authoritative about the user"));
        assert!(prompt.contains("If source actor, evidence, ownership, or durability is unclear"));
        assert!(prompt.contains("Apply every hard rejection gate"));
        assert!(prompt.contains("Drop a candidate if any statement is true"));
        assert!(prompt.contains("primary language of the source user text"));
        assert!(prompt.contains("evidence.quote_or_span must be an exact short quote"));
        assert!(prompt.contains("confidence and importance from you are non-authoritative"));
        assert!(prompt.contains("return {\"facts\":[]}"));
        assert!(prompt.contains("recurring_instruction"));
        assert!(prompt.contains("recurring_user_instruction"));
        assert!(prompt.contains("communication_style"));
        assert!(prompt.contains("communication_preference"));
        assert!(prompt.contains("stable_user_preference"));
        assert!(prompt.contains("project_policy"));
        assert!(prompt.contains("Do not store one-off commands, task progress"));
        assert!(prompt.contains("ontology.fact_class"));
        assert!(prompt.contains("ontology.lifetime_class"));
        assert!(prompt.contains("ontology.evidence_class"));
        assert!(prompt.contains("ontology.proposed_ownership_class"));
        assert!(prompt.contains("explicit_store"));
        assert!(prompt.contains("implicit_candidate"));
        assert!(prompt.contains("## Memory manifest"));
        assert!(prompt.contains("## User text"));
        assert!(prompt.contains("Меня зовут Александр"));
        assert!(!prompt.contains("Available memory tools"));
        assert!(!prompt.contains("memory_search"));
    }
}
