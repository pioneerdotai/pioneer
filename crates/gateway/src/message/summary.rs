use anyhow::{Context, Result};
use pioneer_crud::{ConversationEntry, CrudStore};
use pioneer_provider::{ChatMessage, ChatRequest, ProviderRegistry, ReasoningConfig};
use tracing::debug;

pub struct SummaryConfig {
    pub summary_model: Option<String>,
    pub summary_model_provider: Option<String>,
    pub title_model: Option<String>,
    pub title_model_provider: Option<String>,
}

pub async fn compress_context(
    crud: &CrudStore,
    registry: &ProviderRegistry,
    thread_id: &str,
    entries: &[ConversationEntry],
    existing_summary: Option<&str>,
    target_tokens: usize,
    config: &SummaryConfig,
    fallback_model: Option<&str>,
    fallback_model_provider: Option<&str>,
) -> Result<String> {
    let thread = crud
        .get_thread_by_id(thread_id)
        .await?
        .context("thread not found for context compression")?;

    let prompt = build_compression_prompt(existing_summary, entries, target_tokens);

    let model_provider = config
        .summary_model_provider
        .as_deref()
        .or(fallback_model_provider)
        .unwrap_or(thread.model_provider.as_str());
    let model = config
        .summary_model
        .as_deref()
        .or(fallback_model)
        .unwrap_or(thread.model.as_str());

    let provider =
        registry.get_or_create_for_workspace(thread.workspace_id.as_str(), model_provider)?;

    let request = ChatRequest {
        model: model.to_owned(),
        messages: vec![ChatMessage::user(prompt)],
        temperature: Some(0.3),
        max_tokens: Some(target_tokens as u32),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };

    let response = provider.chat(request).await?;

    if response.text.is_empty() {
        return Ok(existing_summary.unwrap_or("").to_owned());
    }

    let total_covered = entries.len() as i64;
    crud.update_thread_summary(thread_id, &response.text, total_covered)
        .await?;

    debug!(
        thread_id,
        turns_compressed = entries.len(),
        "context compressed"
    );

    Ok(response.text)
}

pub async fn generate_thread_title(
    crud: &CrudStore,
    registry: &ProviderRegistry,
    thread_id: &str,
    user_text: &str,
    config: &SummaryConfig,
) -> Result<Option<String>> {
    let thread = crud
        .get_thread_by_id(thread_id)
        .await?
        .context("thread not found for title generation")?;

    let prompt = build_title_prompt(user_text);

    let model_provider = config
        .title_model_provider
        .as_deref()
        .unwrap_or(thread.model_provider.as_str());
    let model = config
        .title_model
        .as_deref()
        .unwrap_or(thread.model.as_str());

    let provider =
        registry.get_or_create_for_workspace(thread.workspace_id.as_str(), model_provider)?;

    let request = title_generation_chat_request(model, prompt);

    let response = provider.chat(request).await?;
    let title = normalize_generated_title(response.text.as_str());
    if title.is_empty() {
        return Ok(None);
    }

    let title = truncate_utf8_bytes(title.as_str(), 255);
    debug!(
        thread_id,
        title = title.as_str(),
        "thread title candidate generated"
    );

    Ok(Some(title))
}

fn title_generation_chat_request(model: &str, prompt: String) -> ChatRequest {
    ChatRequest {
        model: model.to_owned(),
        messages: vec![ChatMessage::user(prompt)],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: Some(ReasoningConfig::disabled()),
        compiled_prompt: None,
    }
}

fn build_title_prompt(user_text: &str) -> String {
    let mut prompt = String::with_capacity(512);
    prompt.push_str(
        "Generate a concise title (2-6 words) for this conversation. \
         Detect the user's language and return the title in the same language as the user text. \
         No quotes, no emoji, no punctuation at the end. Output only the title.\n\n",
    );

    prompt.push_str("User: ");
    prompt.push_str(user_text);
    prompt.push('\n');

    prompt
}

fn normalize_generated_title(raw: &str) -> String {
    normalize_title_for_compare(raw)
}

pub(super) fn normalize_title_for_compare(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_utf8_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_owned();
    }

    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }

    input[..end].to_owned()
}

fn truncate_utf8_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_owned();
    }

    input.chars().take(max_chars).collect()
}

fn build_compression_prompt(
    existing_summary: Option<&str>,
    entries: &[ConversationEntry],
    target_tokens: usize,
) -> String {
    let mut prompt = String::with_capacity(8192);

    prompt.push_str(&format!(
        "You are a conversation compressor. Your task is to produce a comprehensive summary \
         that captures ALL important context from this conversation. The summary should be \
         approximately {target_tokens} tokens long.\n\n\
         Preserve:\n\
         - Key decisions and their reasoning\n\
         - Technical details, file paths, function names, and code patterns discussed\n\
         - User preferences and corrections\n\
         - Ongoing tasks and their current status\n\
         - Any errors encountered and how they were resolved\n\n\
         Be factual and detailed. Output only the summary text, no preamble or labels.\n\n"
    ));

    if let Some(summary) = existing_summary {
        if !summary.is_empty() {
            prompt.push_str("Previous summary of older conversation:\n");
            prompt.push_str(summary);
            prompt.push_str("\n\n---\n\nConversation to compress:\n\n");
        }
    }

    for entry in entries {
        if let Some(user_text) = &entry.user_text {
            prompt.push_str("User: ");
            prompt.push_str(user_text);
            prompt.push('\n');
        }
        if let Some(assistant_text) = &entry.assistant_text {
            prompt.push_str("Assistant: ");
            // Truncate very long responses to keep prompt manageable
            if assistant_text.chars().count() > 3000 {
                prompt.push_str(truncate_utf8_chars(assistant_text, 3000).as_str());
                prompt.push_str("...[truncated]");
            } else {
                prompt.push_str(assistant_text);
            }
            prompt.push('\n');
        }
        prompt.push('\n');
    }

    prompt
}

#[cfg(test)]
mod tests {
    use super::{
        build_title_prompt, normalize_generated_title, title_generation_chat_request,
        truncate_utf8_bytes,
    };
    use pioneer_provider::ReasoningConfig;

    #[test]
    fn title_normalization_trims_quotes_and_whitespace() {
        assert_eq!(
            normalize_generated_title("  \"Погода   в  Москве\"  "),
            "Погода в Москве"
        );
    }

    #[test]
    fn title_prompt_includes_full_utf8_user_text() {
        let text = "привет".repeat(150);
        let prompt = build_title_prompt(text.as_str());
        assert!(prompt.contains("User: "));
        assert!(prompt.contains(text.as_str()));
        assert!(!prompt.contains("..."));
        assert!(prompt.ends_with('\n'));
    }

    #[test]
    fn title_generation_request_disables_reasoning() {
        let request = title_generation_chat_request("openrouter/model", "title me".to_owned());
        assert_eq!(request.reasoning, Some(ReasoningConfig::disabled()));
        assert!(request.tools.is_none());
    }

    #[test]
    fn truncate_utf8_bytes_respects_char_boundaries() {
        let text = "абвгд";
        assert_eq!(truncate_utf8_bytes(text, 5), "аб");
        assert_eq!(truncate_utf8_bytes(text, 6), "абв");
    }
}
