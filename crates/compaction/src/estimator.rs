//! OpenCode's text heuristic: `Math.round(input.length / 4)` where length is
//! UTF-16 code units. This is an estimate, never a tokenizer or a hard limit.
//! Sizes compose before rounding, so framing and escaped JSON can be prepared
//! once without allocating token IDs or repeatedly scanning growing prefixes.
use serde::Serialize;

pub fn utf16_units(text: &str) -> u64 {
    text.chars().fold(0u64, |units, ch| {
        units.saturating_add(ch.len_utf16() as u64)
    })
}

pub fn tokens_from_utf16(units: u64) -> u64 {
    // Positive Math.round ties go up. Dividing first avoids overflow at MAX.
    units / 4 + u64::from(units % 4 >= 2)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TextSize {
    pub utf16_units: u64,
    pub bytes: usize,
}

impl TextSize {
    pub fn of(text: &str) -> Self {
        Self {
            utf16_units: utf16_units(text),
            bytes: text.len(),
        }
    }

    pub fn tokens(self) -> u64 {
        tokens_from_utf16(self.utf16_units)
    }

    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            utf16_units: self.utf16_units.saturating_add(other.utf16_units),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

/// Size inside JSON quotes (without the two quotes), matching serde_json's
/// escaping. Non-ASCII text remains UTF-8; controls, quote and backslash escape.
pub fn json_string_content_size(text: &str) -> TextSize {
    text.chars().fold(TextSize::default(), |size, ch| {
        let (units, bytes) = match ch {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => (2, 2),
            '\u{0}'..='\u{1f}' => (6, 6),
            _ => (ch.len_utf16() as u64, ch.len_utf8()),
        };
        size.saturating_add(TextSize {
            utf16_units: units,
            bytes,
        })
    })
}

/// Stream serialization into a size sink. No full JSON String/Value copy is
/// retained. Call on media-redacted representations, never on base64 payloads.
pub fn json_size<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<TextSize> {
    struct Sink(TextSize);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.bytes = self.0.bytes.saturating_add(bytes.len());
            // UTF-8 lead bytes count scalar values; four-byte leads add the
            // second UTF-16 unit. This also works across writer chunk boundaries.
            for &byte in bytes {
                let units = if byte < 0x80 {
                    1
                } else if byte >= 0xf0 {
                    2
                } else if byte >= 0xc0 {
                    1
                } else {
                    0
                };
                self.0.utf16_units = self.0.utf16_units.saturating_add(units);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Sink(TextSize::default());
    serde_json::to_writer(&mut sink, value)?;
    Ok(sink.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn javascript_utf16_rounding_including_zero_for_short_nonempty_text() {
        for (text, expected) in [
            ("", 0),
            ("a", 0),
            ("ab", 1),
            ("abcdef", 2),
            ("я", 0),
            ("Привет", 2),
            ("漢字", 1),
            ("e\u{301}", 1),
            ("🌍", 1),
            ("🌍a", 1),
            ("🌍🌍🌍", 2),
            ("<|endoftext|>", 3),
        ] {
            assert_eq!(crate::text_tokens(text), expected, "{text:?}");
        }
        assert_eq!(tokens_from_utf16(u64::MAX), u64::MAX / 4 + 1);
    }

    #[test]
    fn streaming_json_preserves_escaping_and_rounds_after_framing() {
        let text = "a\n\"\\\u{0}🌍漢";
        let encoded = serde_json::to_string(text).unwrap();
        let expected = json_string_content_size(text).saturating_add(TextSize::of("\"\""));
        assert_eq!(json_size(text).unwrap(), expected);
        assert_eq!(expected, TextSize::of(&encoded));
        assert_eq!(
            json_size(&serde_json::json!({"text":text,"items":[1,true]})).unwrap(),
            TextSize::of(&serde_json::json!({"text":text,"items":[1,true]}).to_string())
        );
        assert_eq!(
            TextSize::of("a").saturating_add(TextSize::of("b")).tokens(),
            1
        );
    }
}
