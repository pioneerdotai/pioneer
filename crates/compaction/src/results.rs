use crate::estimator::{TextSize, json_size, json_string_content_size};
#[cfg(test)]
use crate::text_tokens;
use crate::{PAGE_BYTES, PAGE_TOKENS, RESULT_BYTES, RESULT_TOKENS};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResultCursor {
    pub version: String,
    /// UTF-8 byte offset into the immutable textual source.
    pub offset: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultPage {
    pub text: String,
    pub next: Option<ResultCursor>,
    pub eof: bool,
}

/// The caller may publish this representation only after the full source exists.
pub fn result_excerpt(
    text: &str,
    reference: &str,
    available_tokens: u64,
    available_bytes: usize,
) -> anyhow::Result<String> {
    excerpt_in_budget(
        text,
        reference,
        available_tokens.min(RESULT_TOKENS),
        available_bytes.min(RESULT_BYTES),
        TextSize::default(),
        TextSize::of,
    )
}

/// Select text for an already prepared JSON envelope whose text field is empty.
/// Escaping participates in both limits; the source itself is never byte-cut.
pub fn result_excerpt_in_json(
    text: &str,
    reference: &str,
    available_tokens: u64,
    available_bytes: usize,
    empty_envelope: TextSize,
) -> anyhow::Result<String> {
    excerpt_in_budget(
        text,
        reference,
        available_tokens.min(RESULT_TOKENS),
        available_bytes.min(RESULT_BYTES),
        empty_envelope,
        json_string_content_size,
    )
}

fn excerpt_in_budget(
    text: &str,
    reference: &str,
    tokens: u64,
    bytes: usize,
    frame: TextSize,
    size_of: impl Fn(&str) -> TextSize,
) -> anyhow::Result<String> {
    let fits = |size: TextSize| size.bytes <= bytes && size.tokens() <= tokens;
    if text.len() <= bytes && fits(frame.saturating_add(size_of(text))) {
        return Ok(text.into());
    }
    let marker =
        format!("\n[Result shortened. Full source: {reference}; use threads_tools_result_read.]\n");
    let mut size = frame.saturating_add(size_of(&marker));
    anyhow::ensure!(
        fits(size),
        "result reference does not fit; context preparation required"
    );
    // One bounded traversal of the two ends. Combine units before rounding;
    // repeated candidate Strings and full-prefix scans are unnecessary.
    let mut head = 0;
    let mut tail = text.len();
    let mut chars = text.char_indices();
    let mut from_head = true;
    loop {
        let next = if from_head {
            chars.next()
        } else {
            chars.next_back()
        };
        let Some((offset, ch)) = next else { break };
        let next_size = size.saturating_add(size_of(&text[offset..offset + ch.len_utf8()]));
        if !fits(next_size) {
            break;
        }
        size = next_size;
        if from_head {
            head = offset + ch.len_utf8();
        } else {
            tail = offset;
        }
        from_head = !from_head;
    }
    Ok(format!("{}{}{}", &text[..head], marker, &text[tail..]))
}

/// Framing and continuation fields participate in both limits. This routine also
/// accepts a smaller caller budget; it never splits UTF-8 or trusts a stale cursor.
pub fn result_page(
    text: &str,
    version: &str,
    cursor: Option<&ResultCursor>,
    available_tokens: u64,
    available_bytes: usize,
) -> anyhow::Result<ResultPage> {
    result_page_in_budget(
        text,
        version,
        cursor,
        available_tokens.min(PAGE_TOKENS),
        available_bytes.min(PAGE_BYTES),
    )
}

/// Exact caller budget without compaction's excerpt/page defaults. Thread tools
/// own their complete projection envelope and must not inherit smaller ceilings.
/// Other consumers retain result_page's existing behavior.
pub fn result_page_in_budget(
    text: &str,
    version: &str,
    cursor: Option<&ResultCursor>,
    available_tokens: u64,
    available_bytes: usize,
) -> anyhow::Result<ResultPage> {
    let tokens = available_tokens;
    let bytes = available_bytes;
    let start = if let Some(cursor) = cursor {
        anyhow::ensure!(cursor.version == version, "stale result cursor");
        cursor.offset
    } else {
        0
    };
    anyhow::ensure!(
        start <= text.len() && text.is_char_boundary(start),
        "invalid result cursor"
    );
    let make = |end: usize| ResultPage {
        text: text[start..end].into(),
        next: (end < text.len()).then(|| ResultCursor {
            version: version.into(),
            offset: end,
        }),
        eof: end == text.len(),
    };
    // EOF removes the cursor envelope, so consider it separately. Only scan
    // the full remainder when it can fit the independent hard byte budget.
    if text.len().saturating_sub(start) <= bytes {
        let content = json_string_content_size(&text[start..]);
        let eof_frame = json_size(&ResultPage {
            text: String::new(),
            next: None,
            eof: true,
        })?
        .saturating_add(content);
        if eof_frame.bytes <= bytes && eof_frame.tokens() <= tokens {
            return Ok(make(text.len()));
        }
    }
    let framing = json_size(&ResultPage {
        text: String::new(),
        next: Some(ResultCursor {
            version: version.into(),
            offset: 0,
        }),
        eof: false,
    })?;
    let mut content = TextSize::default();
    let mut best = None;
    for (offset, ch) in text[start..].char_indices() {
        let end = start + offset + ch.len_utf8();
        if end == text.len() {
            break;
        } // EOF was considered above.
        content = content.saturating_add(json_string_content_size(&text[start + offset..end]));
        let digit_extra = end.ilog10() as usize; // offset 0 frame has one digit.
        let size = framing.saturating_add(content).saturating_add(TextSize {
            utf16_units: digit_extra as u64,
            bytes: digit_extra,
        });
        if size.bytes > bytes || size.tokens() > tokens {
            break;
        }
        best = Some(end);
    }
    let best = best.map(make);
    let page = best.ok_or_else(|| anyhow::anyhow!("result page framing does not fit"))?;
    anyhow::ensure!(
        page.eof || !page.text.is_empty(),
        "result page cannot make progress within budget"
    );
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn excerpt_preserves_both_ends_and_counts_marker() {
        let source = format!("BEGIN{}END", "🌍漢字 lots of output ".repeat(10_000));
        for (tokens, bytes) in [(8192, 65536), (20_000, 500), (300, 65536)] {
            let result = result_excerpt(&source, "result://fixture", tokens, bytes).unwrap();
            assert!(result.starts_with("BEGIN") && result.ends_with("END"));
            assert!(result.contains("threads_tools_result_read"));
            assert!(
                result.len() <= bytes.min(RESULT_BYTES)
                    && text_tokens(&result) <= tokens.min(RESULT_TOKENS)
            );
        }
        assert!(result_excerpt(&source, "ref", 1, 1).is_err());
    }
    #[test]
    fn json_excerpt_keeps_escaped_content_inside_the_hard_envelope_budget() {
        let text = format!("BEGIN{}END", "\n\"\\\u{0}🌍".repeat(1000));
        let frame = json_size(&serde_json::json!({"text":"", "reference":"ref"})).unwrap();
        let result = result_excerpt_in_json(&text, "ref", 200, 700, frame).unwrap();
        let wire = serde_json::json!({"text":result,"reference":"ref"}).to_string();
        assert!(wire.len() <= 700 && text_tokens(&wire) <= 200);
        assert!(result.starts_with("BEGIN") && result.ends_with("END"));
        assert!(result.contains("threads_tools_result_read"));
        let empty = result_page("", "v", None, 100, 100).unwrap();
        assert!(empty.eof && empty.next.is_none() && empty.text.is_empty());
        assert!(result_page("🌍", "v", None, 1, 1).is_err());
    }
    #[test]
    fn pages_reconstruct_exact_unicode_with_framing_and_version() {
        let text = "日本語 🌍\\\"\n".repeat(1000);
        let mut next = None;
        let mut rebuilt = String::new();
        loop {
            let page = result_page(&text, "v1", next.as_ref(), 200, 1000).unwrap();
            let encoded = serde_json::to_string(&page).unwrap();
            assert!(encoded.len() <= 1000 && text_tokens(&encoded) <= 200);
            rebuilt.push_str(&page.text);
            next = page.next;
            if page.eof {
                break;
            }
        }
        assert_eq!(text, rebuilt);
        assert!(
            result_page(
                &text,
                "v2",
                Some(&ResultCursor {
                    version: "v1".into(),
                    offset: 0
                }),
                200,
                1000
            )
            .is_err()
        );
        assert!(
            result_page(
                &text,
                "v1",
                Some(&ResultCursor {
                    version: "v1".into(),
                    offset: 1
                }),
                200,
                1000
            )
            .is_err()
        );
    }
}
