use std::sync::OnceLock;
use tiktoken_rs::CoreBPE;

/// Lazily initialized cl100k_base tokenizer (used by GPT-4, GPT-4o, etc.).
/// Works as a reasonable approximation for all LLM providers.
fn bpe() -> &'static CoreBPE {
    static BPE: OnceLock<CoreBPE> = OnceLock::new();
    BPE.get_or_init(|| {
        tiktoken_rs::cl100k_base().expect("failed to initialize cl100k_base tokenizer")
    })
}

pub(crate) fn count_tokens(text: &str) -> usize {
    bpe().encode_with_special_tokens(text).len()
}

#[cfg(test)]
mod tests {
    use super::count_tokens;

    #[test]
    fn empty_string_is_zero_tokens() {
        assert_eq!(count_tokens(""), 0);
    }

    #[test]
    fn simple_english_text_produces_tokens() {
        let count = count_tokens("Hello, world!");
        assert!(count > 0 && count < 10);
    }

    #[test]
    fn longer_text_produces_more_tokens() {
        let short = count_tokens("Hi");
        let long = count_tokens(
            "This is a much longer sentence that should produce more tokens than just hi.",
        );
        assert!(long > short);
    }
}
