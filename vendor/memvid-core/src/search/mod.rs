mod parser;

#[cfg(feature = "lex")]
mod tantivy;

use crate::types::Frame;
use parser::{Expr, FieldTerm, Term, TextTerm};

pub(crate) use parser::parse_query;
pub(crate) use parser::{DateRange, ParsedQuery};

#[cfg(feature = "lex")]
#[allow(unused_imports)]
pub(crate) use tantivy::{
    EmbeddedLexSegment, EmbeddedLexStorage, LexWalBatch, TantivyEngine, TantivySnapshot,
};

pub struct EvaluationContext<'a> {
    pub frame: &'a Frame,
    pub content_lower: &'a str,
}

impl ParsedQuery {
    pub fn evaluate(&self, ctx: &EvaluationContext<'_>) -> bool {
        self.expr.evaluate(ctx)
    }

    pub fn text_tokens(&self) -> Vec<String> {
        self.expr.collect_tokens()
    }

    pub fn required_date_range(&self) -> Option<DateRange> {
        self.expr.required_date_range()
    }

    pub fn contains_field_terms(&self) -> bool {
        self.expr.contains_field_terms()
    }
}

impl TextTerm {
    pub(crate) fn matches(&self, haystack: &str) -> bool {
        match self {
            TextTerm::Word(word) => {
                let needle = word.to_lowercase();
                haystack.contains(&needle)
            }
            TextTerm::Phrase(phrase) => {
                let needle = phrase.to_lowercase();
                haystack.contains(&needle)
            }
            TextTerm::Wildcard(pattern) => pattern.regex.is_match(haystack),
        }
    }
}

impl FieldTerm {
    pub(crate) fn matches(&self, ctx: &EvaluationContext<'_>) -> bool {
        match self {
            FieldTerm::Uri(value) => ctx
                .frame
                .uri
                .as_deref()
                .is_some_and(|uri| uri.eq_ignore_ascii_case(value)),
            FieldTerm::Scope(prefix) => ctx
                .frame
                .uri
                .as_deref()
                .is_some_and(|uri| uri.starts_with(prefix)),
            FieldTerm::Track(track) => ctx
                .frame
                .track
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(track)),
            FieldTerm::Tag(tag) => ctx
                .frame
                .tags
                .iter()
                .any(|value| value.eq_ignore_ascii_case(tag)),
            FieldTerm::Label(label) => ctx
                .frame
                .labels
                .iter()
                .any(|value| value.eq_ignore_ascii_case(label)),
            FieldTerm::DateRange(range) => range.matches(ctx.frame),
        }
    }
}

impl DateRange {
    fn matches(&self, frame: &Frame) -> bool {
        if self.start.is_none() && self.end.is_none() {
            return true;
        }
        let mut candidates = Vec::new();
        candidates.push(frame.timestamp);
        #[cfg(feature = "temporal_track")]
        if let Some(anchor_ts) = frame.anchor_ts {
            candidates.push(anchor_ts);
        }
        for date_str in &frame.content_dates {
            if let Some(ts) = parser::parse_date_value(date_str) {
                candidates.push(ts);
            }
        }
        candidates.into_iter().any(|ts| self.contains(ts))
    }

    pub(crate) fn contains(&self, timestamp: i64) -> bool {
        if let Some(start) = self.start {
            if timestamp < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if timestamp > end {
                return false;
            }
        }
        true
    }

    pub fn intersection(&self, other: &Self) -> Self {
        let start = match (self.start, other.start) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let end = match (self.end, other.end) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Self { start, end }
    }

    pub fn is_empty(&self) -> bool {
        match (self.start, self.end) {
            (Some(start), Some(end)) => start > end,
            _ => false,
        }
    }
}

impl Expr {
    fn evaluate(&self, ctx: &EvaluationContext<'_>) -> bool {
        match self {
            Expr::Or(children) => children.iter().any(|child| child.evaluate(ctx)),
            Expr::And(children) => children.iter().all(|child| child.evaluate(ctx)),
            Expr::Not(child) => !child.evaluate(ctx),
            Expr::Term(term) => term.evaluate(ctx),
        }
    }

    fn collect_tokens(&self) -> Vec<String> {
        let mut tokens = Vec::new();
        self.collect_into(&mut tokens);
        tokens
    }

    fn collect_into(&self, tokens: &mut Vec<String>) {
        match self {
            Expr::Or(children) | Expr::And(children) => {
                for child in children {
                    child.collect_into(tokens);
                }
            }
            Expr::Not(child) => child.collect_into(tokens),
            Expr::Term(Term::Text(text)) => match text {
                TextTerm::Word(word) | TextTerm::Phrase(word) => tokens.push(word.clone()),
                TextTerm::Wildcard(pattern) => {
                    if let Some(seed) = pattern.seed() {
                        tokens.push(seed);
                    }
                }
            },
            Expr::Term(Term::Field(_)) => {}
        }
    }

    fn required_date_range(&self) -> Option<DateRange> {
        match self {
            Expr::Term(Term::Field(FieldTerm::DateRange(range))) => Some(range.clone()),
            Expr::And(children) => {
                let mut combined: Option<DateRange> = None;
                for child in children {
                    if let Some(child_range) = child.required_date_range() {
                        combined = Some(match combined {
                            Some(existing) => existing.intersection(&child_range),
                            None => child_range,
                        });
                    }
                }
                combined
            }
            Expr::Or(_) | Expr::Not(_) => None,
            Expr::Term(_) => None,
        }
    }

    fn contains_field_terms(&self) -> bool {
        match self {
            Expr::Or(children) | Expr::And(children) => {
                children.iter().any(parser::Expr::contains_field_terms)
            }
            Expr::Not(child) => child.contains_field_terms(),
            Expr::Term(term) => term.contains_field_terms(),
        }
    }
}

impl Term {
    fn evaluate(&self, ctx: &EvaluationContext<'_>) -> bool {
        match self {
            Term::Text(text) => text.matches(ctx.content_lower),
            Term::Field(field) => field.matches(ctx),
        }
    }

    fn contains_field_terms(&self) -> bool {
        matches!(self, Term::Field(_))
    }
}
