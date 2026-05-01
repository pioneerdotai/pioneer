use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MARKDOWN_AST_VERSION: u16 = 1;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkdownDocument {
    #[serde(default)]
    pub blocks: Vec<MarkdownBlock>,
}

impl MarkdownDocument {
    pub fn from_plain_text(text: impl Into<String>) -> Self {
        Self {
            blocks: vec![MarkdownBlock::Paragraph(MarkdownInline::plain(text))],
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MarkdownBlock {
    Paragraph(MarkdownInline),
    #[serde(rename_all = "camelCase")]
    Heading {
        level: u8,
        content: MarkdownInline,
    },
    List(MarkdownList),
    #[serde(rename_all = "camelCase")]
    Quote {
        blocks: Vec<MarkdownBlock>,
    },
    #[serde(rename_all = "camelCase")]
    Code {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
        text: String,
    },
    Rule,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MarkdownList {
    pub ordered: bool,
    pub start: usize,
    #[serde(default)]
    pub items: Vec<MarkdownListItem>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MarkdownListItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked: Option<bool>,
    #[serde(default)]
    pub blocks: Vec<MarkdownBlock>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Default, PartialEq, Eq)]
pub struct MarkdownInline {
    pub text: String,
    #[serde(default)]
    pub marks: Vec<MarkdownMark>,
}

impl MarkdownInline {
    pub fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            marks: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct MarkdownMark {
    pub start: usize,
    pub end: usize,
    pub kind: MarkdownMarkKind,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum MarkdownMarkKind {
    Bold,
    Italic,
    Strike,
    Code,
    #[serde(rename_all = "camelCase")]
    Link {
        url: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{MarkdownBlock, MarkdownDocument, MarkdownInline};

    #[test]
    fn quote_blocks_round_trip_json() {
        let document = MarkdownDocument {
            blocks: vec![MarkdownBlock::Quote {
                blocks: vec![MarkdownBlock::Paragraph(MarkdownInline::plain("hello"))],
            }],
        };

        let json = serde_json::to_string(&document).expect("markdown document should serialize");
        assert!(json.contains("\"type\":\"quote\""));

        let round_trip: MarkdownDocument =
            serde_json::from_str(&json).expect("markdown document should deserialize");
        assert_eq!(round_trip, document);
    }
}
