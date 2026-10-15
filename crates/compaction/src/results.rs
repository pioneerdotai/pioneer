use crate::{PAGE_BYTES, PAGE_TOKENS, RESULT_BYTES, RESULT_TOKENS, text_tokens};
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

fn fits(text: &str, tokens: u64, bytes: usize) -> bool {
    text.len() <= bytes && text_tokens(text) <= tokens
}
fn boundary_before(text: &str, mut offset: usize) -> usize {
    offset = offset.min(text.len());
    while !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

/// The caller may publish this representation only after the full source exists.
pub fn result_excerpt(
    text: &str,
    reference: &str,
    available_tokens: u64,
    available_bytes: usize,
) -> anyhow::Result<String> {
    let tokens = available_tokens.min(RESULT_TOKENS);
    let bytes = available_bytes.min(RESULT_BYTES);
    if fits(text, tokens, bytes) {
        return Ok(text.into());
    }
    let marker =
        format!("\n[Result shortened. Full source: {reference}; use threads_tools_result_read.]\n");
    anyhow::ensure!(
        fits(&marker, tokens, bytes),
        "result reference does not fit; context preparation required"
    );
    let mut low = 0;
    let mut high = text.len().min(bytes);
    let mut best = marker.clone();
    while low <= high {
        let size = low + (high - low) / 2;
        let head = boundary_before(text, size.div_ceil(2));
        let tail = boundary_before(text, text.len().saturating_sub(size / 2));
        let candidate = format!("{}{}{}", &text[..head], marker, &text[tail..]);
        if fits(&candidate, tokens, bytes) {
            best = candidate;
            low = size + 1;
        } else if size == 0 {
            break;
        } else {
            high = size - 1;
        }
    }
    Ok(best)
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
    let tokens = available_tokens.min(PAGE_TOKENS);
    let bytes = available_bytes.min(PAGE_BYTES);
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
    let mut low = start;
    let mut high = text.len().min(start.saturating_add(bytes));
    let mut best = None;
    while low <= high {
        let mid = low + (high - low) / 2;
        let end = boundary_before(text, mid);
        let candidate = make(end);
        let encoded = serde_json::to_string(&candidate)?;
        if fits(&encoded, tokens, bytes) {
            best = Some(candidate);
            low = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
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
