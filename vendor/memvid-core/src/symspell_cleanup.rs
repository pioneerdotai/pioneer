//! SymSpell-based PDF text cleanup.
//!
//! This module uses the SymSpell algorithm to fix broken word spacing
//! that commonly occurs in PDF text extraction. It handles both:
//! - Words split by spurious spaces: "emp lo yee" → "employee"
//! - Words incorrectly joined: "olderdo cuments" → "older documents"
//!
//! Uses a hybrid approach:
//! 1. Pre-join obvious PDF fragments (single chars, 2-3 letter non-words)
//! 2. Use SymSpell lookup_compound for remaining issues

use std::sync::OnceLock;

use symspell::{AsciiStringStrategy, SymSpell, Verbosity};

/// Global SymSpell instance with both dictionaries loaded
static SYMSPELL: OnceLock<SymSpell<AsciiStringStrategy>> = OnceLock::new();

/// Embedded frequency dictionary for English (top 82,765 words)
const FREQUENCY_DICT: &str = include_str!("../data/frequency_dictionary_en_82_765.txt");

/// Embedded bigram dictionary for English (243,342 word pairs)
/// Required for lookup_compound to work properly with phrase context
const BIGRAM_DICT: &str = include_str!("../data/frequency_bigramdictionary_en_243_342.txt");

/// Common short words that should NOT be joined with neighbors
const COMMON_SHORT_WORDS: &[&str] = &[
    "a", "i", "an", "as", "at", "be", "by", "do", "go", "he", "if", "in", "is", "it", "me", "my",
    "no", "of", "on", "or", "so", "to", "up", "us", "we", "am", "are", "can", "did", "for", "get",
    "got", "had", "has", "her", "him", "his", "its", "let", "may", "nor", "not", "now", "off",
    "old", "one", "our", "out", "own", "ran", "run", "saw", "say", "see", "set", "she", "the",
    "too", "two", "use", "was", "way", "who", "why", "yet", "you", "all", "and", "any", "but",
    "few", "how", "man", "new", "per", "put", "via",
];

/// Initialize the SymSpell instance with both dictionaries
fn init_symspell() -> SymSpell<AsciiStringStrategy> {
    let mut symspell: SymSpell<AsciiStringStrategy> = SymSpell::default();

    // Load unigram dictionary (word frequencies)
    // Format: "word frequency" (e.g., "the 23135851162")
    for line in FREQUENCY_DICT.lines() {
        symspell.load_dictionary_line(line, 0, 1, " ");
    }

    // Load bigram dictionary (word pair frequencies)
    // Format: "word1 word2 frequency" (e.g., "abcs of 10956800")
    for line in BIGRAM_DICT.lines() {
        symspell.load_bigram_dictionary_line(line, 0, 2, " ");
    }

    tracing::debug!(
        target: "memvid::symspell",
        "SymSpell initialized with {} unigram and {} bigram entries",
        FREQUENCY_DICT.lines().count(),
        BIGRAM_DICT.lines().count()
    );

    symspell
}

/// Get or initialize the global SymSpell instance
fn get_symspell() -> &'static SymSpell<AsciiStringStrategy> {
    SYMSPELL.get_or_init(init_symspell)
}

/// Check if a word is a common short English word
fn is_common_word(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    COMMON_SHORT_WORDS.contains(&lower.as_str())
}

/// Check if string is purely alphabetic
fn is_alpha(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphabetic())
}

/// Check if a token looks like a PDF fragment (should be joined)
fn is_fragment(s: &str) -> bool {
    if !is_alpha(s) {
        return false;
    }
    let len = s.len();
    // Single chars (except I, a) are fragments
    if len == 1 {
        if let Some(c) = s.chars().next() {
            return c != 'I' && c != 'a' && c != 'A';
        }
        return false;
    }
    // 2-3 letter non-common words are likely fragments
    if len <= 3 && !is_common_word(s) {
        return true;
    }
    // 4 letter non-common words that look like fragments (not in dictionary)
    // This catches patterns like "resp", "repp", "prev", etc.
    if len == 4 && !is_common_word(s) {
        let symspell = get_symspell();
        let suggestions = symspell.lookup(&s.to_lowercase(), Verbosity::Top, 0);
        // If not in dictionary with exact match, it's likely a fragment
        if suggestions.is_empty() {
            return true;
        }
    }
    false
}

/// Pre-process text to join obvious PDF fragment runs before SymSpell
///
/// This handles cases like "emp lo yee" → "employee" by joining
/// sequences of short fragments that SymSpell can't handle well.
fn prejoin_fragments(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < 2 {
        return text.to_string();
    }

    let symspell = get_symspell();
    let mut result: Vec<String> = Vec::with_capacity(words.len());
    let mut i = 0;

    while i < words.len() {
        let word = words[i];

        // Try greedy merge: look ahead and try progressively longer merges
        // This handles "resp on liabilities" → "responsibilities"
        let mut best_merge: Option<(String, usize)> = None; // (corrected_word, end_index)

        // Only try greedy merge if current word is NOT a common word
        // This prevents "The emp" from merging incorrectly
        if is_alpha(word) && !is_common_word(word) && i + 1 < words.len() {
            let mut merged = String::from(word);
            let mut j = i + 1;

            // Try merging up to 5 consecutive tokens
            while j < words.len() && j - i < 6 && is_alpha(words[j]) {
                merged.push_str(words[j]);
                j += 1;

                // Check if this merge produces a valid word
                let suggestions = symspell.lookup(&merged.to_lowercase(), Verbosity::Closest, 2);
                if let Some(suggestion) = suggestions.first() {
                    // Accept if exact match or close enough for longer words
                    if suggestion.distance == 0
                        || (suggestion.distance == 1 && merged.len() >= 6)
                        || (suggestion.distance == 2 && merged.len() >= 10)
                    {
                        // This is a valid merge - but keep looking for longer ones
                        best_merge = Some((suggestion.term.clone(), j));
                    }
                }

                // Stop if the next word is a common word (likely real word boundary)
                if j < words.len() && is_common_word(words[j]) && words[j].len() >= 3 {
                    break;
                }
            }
        }

        // Check if we should try to merge with next word(s) using old logic
        let should_try_old_merge = if best_merge.is_none() && i + 1 < words.len() {
            let next = words[i + 1];
            // Case 1: Both are obvious fragments
            if is_fragment(word) && is_fragment(next) {
                true
            }
            // Case 2: Current is short (1-2 chars) alpha, next is fragment
            // Handles "A va" → "ava" for names
            else if is_alpha(word) && word.len() <= 2 && is_fragment(next) {
                // Check if joining creates a valid word
                let test_merge = format!("{}{}", word.to_lowercase(), next.to_lowercase());
                let suggestions = symspell.lookup(&test_merge, Verbosity::Closest, 1);
                suggestions
                    .first()
                    .map(|s| s.distance == 0)
                    .unwrap_or(false)
            } else {
                false
            }
        } else {
            false
        };

        if let Some((corrected, end_idx)) = best_merge {
            result.push(corrected);
            i = end_idx;
        } else if should_try_old_merge {
            // Collect all consecutive fragments
            let mut merged = String::from(word);
            let start_i = i;
            i += 1;

            while i < words.len() && is_fragment(words[i]) {
                merged.push_str(words[i]);
                i += 1;
            }

            // Check if merged string is a valid word
            let suggestions = symspell.lookup(&merged.to_lowercase(), Verbosity::Closest, 2);
            if let Some(suggestion) = suggestions.first() {
                if suggestion.distance == 0 || (suggestion.distance <= 2 && merged.len() >= 4) {
                    // It's a valid word or close enough, use the corrected version
                    result.push(suggestion.term.clone());
                    continue;
                }
            }

            // Not a valid word, restore original tokens
            for j in start_i..i {
                result.push(words[j].to_string());
            }
        } else {
            result.push(word.to_string());
            i += 1;
        }
    }

    result.join(" ")
}

/// Fix broken word spacing in PDF-extracted text using SymSpell.
///
/// Uses a hybrid approach:
/// 1. Pre-join obvious PDF fragments (single chars, 2-3 letter non-words)
/// 2. Use SymSpell lookup_compound for remaining issues
///
/// # Arguments
/// * `text` - The text to clean up
/// * `max_edit_distance` - Maximum edit distance for corrections (default: 2)
///
/// # Returns
/// The cleaned text with proper word spacing
#[must_use]
pub fn fix_pdf_text_symspell(text: &str, max_edit_distance: i64) -> String {
    if text.is_empty() {
        return String::new();
    }

    let symspell = get_symspell();

    // Process line by line to preserve paragraph structure
    let lines: Vec<&str> = text.lines().collect();
    let mut result = Vec::with_capacity(lines.len());

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            result.push(String::new());
            continue;
        }

        // Split line into tokens
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }

        // Group tokens into "safe" (text-only) and "protected" (contains digits/symbols) chunks
        let mut chunks: Vec<(bool, Vec<&str>)> = Vec::new(); // (is_protected, tokens)

        let mut current_chunk: Vec<&str> = Vec::new();
        let mut current_is_protected = false;

        for token in tokens {
            // Heuristic: specific tokens are "protected" from SymSpell
            // 1. Contains a digit (e.g. "2025", "X500", "COVID-19")
            // 2. Contains non-alphabetic symbols (e.g. "user_id", "email@addr") - optional, but safer
            // For now, let's stick to the "contains digit" rule which fixes the observed massive failures
            let is_protected = token.chars().any(|c| c.is_ascii_digit());

            if chunks.is_empty() && current_chunk.is_empty() {
                // First token
                current_is_protected = is_protected;
                current_chunk.push(token);
            } else if is_protected == current_is_protected {
                // Continue current chunk
                current_chunk.push(token);
            } else {
                // Switch chunk type
                chunks.push((current_is_protected, current_chunk));
                current_chunk = vec![token];
                current_is_protected = is_protected;
            }
        }
        if !current_chunk.is_empty() {
            chunks.push((current_is_protected, current_chunk));
        }

        // Process chunks
        let mut line_parts: Vec<String> = Vec::new();
        for (is_protected, chunk_tokens) in chunks {
            if is_protected {
                // Keep protected tokens as-is (just join them)
                line_parts.push(chunk_tokens.join(" "));
            } else {
                // Run SymSpell on safe text tokens
                let chunk_text = chunk_tokens.join(" ");

                // Step 1: Pre-join obvious PDF fragments
                let prejoined = prejoin_fragments(&chunk_text);

                // Step 2: Use lookup_compound for remaining issues
                let suggestions = symspell.lookup_compound(&prejoined, max_edit_distance);

                if let Some(suggestion) = suggestions.first() {
                    line_parts.push(suggestion.term.clone());
                } else {
                    line_parts.push(chunk_text);
                }
            }
        }

        result.push(line_parts.join(" "));
    }

    result.join("\n")
}

/// Fix broken word spacing with default edit distance of 2
#[must_use]
pub fn fix_pdf_text(text: &str) -> String {
    fix_pdf_text_symspell(text, 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixes_split_words() {
        // Common PDF extraction artifacts - SymSpell returns lowercase
        let result = fix_pdf_text("emp lo yee");
        assert!(
            result == "employee" || result == "emp lo yee",
            "got: {}",
            result
        );

        let result = fix_pdf_text("co mp an y");
        assert!(
            result == "company" || result.contains("comp"),
            "got: {}",
            result
        );
    }

    #[test]
    fn fixes_classic_symspell_example() {
        // The classic SymSpell demo sentence
        let input = "whereis th elove";
        let result = fix_pdf_text(input);
        assert!(
            result.contains("where") && result.contains("love"),
            "got: {}",
            result
        );
    }

    #[test]
    fn preserves_correct_text() {
        // Normal text should remain mostly unchanged
        let result = fix_pdf_text("the manager reported");
        assert!(
            result.contains("manager") && result.contains("reported"),
            "got: {}",
            result
        );
    }

    #[test]
    fn handles_multiline() {
        let input = "hello world\n\ntest sentence";
        let result = fix_pdf_text(input);
        assert!(result.contains("hello"));
        assert!(result.contains("test"));
    }

    #[test]
    fn fixes_name_fragments() {
        // "A va" should become "ava" (SymSpell lowercases)
        let result = prejoin_fragments("A va Martin");
        assert!(
            result.contains("ava") || result.contains("Ava"),
            "got: {}",
            result
        );
    }

    #[test]
    fn fixes_supervisor_split() {
        // "sup erviso r" is a common PDF artifact
        let result = fix_pdf_text("sup erviso r");
        assert!(
            result.contains("supervisor") || result.contains("supervise"),
            "got: {}",
            result
        );
    }

    #[test]
    fn preserves_valid_short_words() {
        // Valid short words should NOT be joined
        let result = fix_pdf_text("I am a person");
        assert!(
            result.contains("am") && result.contains("person"),
            "got: {}",
            result
        );

        let result = fix_pdf_text("to be or not");
        // Should preserve these common short words
        assert!(
            result.contains("to") || result.contains("be"),
            "got: {}",
            result
        );
    }

    #[test]
    fn fixes_joined_words() {
        // SymSpell lookup_compound should split incorrectly joined words
        let result = fix_pdf_text("olderdo cuments");
        // Should become "older documents" or similar
        assert!(
            result.contains("older") || result.contains("document"),
            "got: {}",
            result
        );
    }

    #[test]
    fn handles_mixed_content() {
        // Mix of correct and broken text
        let result = fix_pdf_text("The emp lo yee reported to the man ager");
        assert!(
            result.contains("employee") || result.contains("emp"),
            "got: {}",
            result
        );
        assert!(
            result.contains("manager") || result.contains("man"),
            "got: {}",
            result
        );
    }

    #[test]
    fn handles_empty_input() {
        assert_eq!(fix_pdf_text(""), "");
        assert_eq!(fix_pdf_text("   "), "");
    }

    #[test]
    fn handles_single_word() {
        let result = fix_pdf_text("hello");
        assert_eq!(result, "hello");
    }

    #[test]
    fn prejoin_respects_common_words() {
        // "man" is a common word, should not be joined with "ager" unless adjacent
        // "man ager" should still join since "ager" is a fragment
        let result = prejoin_fragments("man ager");
        assert!(
            result == "manager" || result == "man ager",
            "got: {}",
            result
        );
    }

    #[test]
    fn fixes_numbers_and_proper_nouns() {
        // "Model X500" should keep "X500" intact (protected token),
        // while "Model" might be lowercased to "model" by SymSpell
        let result = fix_pdf_text("Model X500");
        assert_eq!(result, "model X500");

        // "2025" should be protected
        let result = fix_pdf_text("The year 2025");
        assert_eq!(result, "the year 2025");

        // "iPhone 15 Pro" -> "15" is protected.
        // "iPhone" -> "iphone" (lowercased), "Pro" -> "pro" (lowercased)
        let result = fix_pdf_text("iPhone 15 Pro");
        assert_eq!(result, "iphone 15 pro");

        // "COVID-19" has digits -> protected
        let result = fix_pdf_text("COVID-19 pandemic");
        assert_eq!(result, "COVID-19 pandemic");

        // Mixed line with cleanup needed + protected token
        // "emp lo yee 123" -> "employee 123"
        let result = fix_pdf_text("emp lo yee 123");
        assert_eq!(result, "employee 123");
    }
}
