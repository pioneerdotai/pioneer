#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryTurnPolicyClassifierPromptInput {
    pub user_input: String,
    pub thread_mode_label: String,
    pub memory_enabled: bool,
    pub classifier_fallback_label: String,
}

pub fn render_memory_turn_policy_classifier_prompt(
    input: &MemoryTurnPolicyClassifierPromptInput,
) -> String {
    format!(
        concat!(
            "You are Pioneer memory turn policy classifier.\n",
            "Classify only the current user input. Do not answer the user. Do not search memory. Do not use durable memory as evidence.\n",
            "The user may write in any language. Distinguish direct requests from quoted examples, code, logs, or hypothetical text.\n",
            "Memory enabled: {memory_enabled}. Thread mode: {thread_mode_label}. Fallback if invalid: {fallback}.\n\n",
            "Return strict JSON only, with exactly this shape:\n",
            "{{\n",
            "  \"intent\": \"normal | memory_no_use | memory_no_save | explicit_remember | explicit_forget | mixed\",\n",
            "  \"recall\": \"allow | disabled\",\n",
            "  \"prompt\": \"full | read_only | forget_only | disabled\",\n",
            "  \"readTools\": \"allow | forget_only | disabled\",\n",
            "  \"rememberTool\": \"allow | disabled\",\n",
            "  \"forgetTool\": \"allow | disabled\",\n",
            "  \"postTurnExtraction\": \"allow | disabled\",\n",
            "  \"activeMemory\": \"allow | disabled\",\n",
            "  \"explicitRemember\": false,\n",
            "  \"explicitForget\": false,\n",
            "  \"forgetTargetHint\": null,\n",
            "  \"language\": \"und\",\n",
            "  \"confidence\": 0.0,\n",
            "  \"reasonCode\": \"default_allow_read\"\n",
            "}}\n\n",
            "Policy semantics:\n",
            "- normal: allow recall, full prompt, read tools, memory_remember, and memory_forget. memory_remember may be used proactively for stable durable future-useful facts.\n",
            "- memory_no_use: disable recall, prompt, read tools, memory_remember, memory_forget, post-turn extraction, and active memory.\n",
            "- memory_no_save: allow recall/read tools but disable memory_remember and post-turn extraction. memory_forget may remain allowed.\n",
            "- explicit_remember: allow memory_remember and normal read tools.\n",
            "- explicit_forget: disable broad recall, use forget_only prompt/read tools, allow memory_forget, disable memory_remember.\n",
            "- mixed: use the narrowest policy that satisfies the explicit user request, for example no-use plus forget target becomes forget-only.\n",
            "When uncertain, prefer normal/default allow unless the input clearly restricts memory use or memory writes.\n\n",
            "Current user input:\n",
            "{user_input}\n"
        ),
        memory_enabled = input.memory_enabled,
        thread_mode_label = input.thread_mode_label.trim(),
        fallback = input.classifier_fallback_label.trim(),
        user_input = input.user_input
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_turn_policy_prompt_requires_json_and_arbitrary_language() {
        let prompt =
            render_memory_turn_policy_classifier_prompt(&MemoryTurnPolicyClassifierPromptInput {
                user_input: "No guardes esto.".to_owned(),
                thread_mode_label: "agent".to_owned(),
                memory_enabled: true,
                classifier_fallback_label: "default_allow".to_owned(),
            });

        assert!(prompt.contains("any language"));
        assert!(prompt.contains("Return strict JSON only"));
        assert!(prompt.contains("memory_no_use"));
        assert!(prompt.contains("memory_no_save"));
        assert!(prompt.contains("explicit_remember"));
        assert!(prompt.contains("explicit_forget"));
        assert!(prompt.contains("Do not search memory"));
        assert!(prompt.contains("No guardes esto."));
        assert!(!prompt.contains("## Memory Recall"));
        assert!(!prompt.contains("Relevant memories"));
        assert!(!prompt.contains("\"tools\""));
    }

    #[test]
    fn memory_turn_policy_prompt_documents_proactive_writes() {
        let prompt =
            render_memory_turn_policy_classifier_prompt(&MemoryTurnPolicyClassifierPromptInput {
                user_input: "I prefer short answers.".to_owned(),
                thread_mode_label: "agent".to_owned(),
                memory_enabled: true,
                classifier_fallback_label: "default_allow".to_owned(),
            });

        assert!(prompt.contains("memory_remember may be used proactively"));
        assert!(prompt.contains("stable durable future-useful facts"));
    }
}
