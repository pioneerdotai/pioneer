//! Rules-based enrichment engine using regex patterns.
//!
//! This engine extracts memory cards from text using configurable regex
//! patterns. It's fast, deterministic, and doesn't require any models.

use super::{EnrichmentContext, EnrichmentEngine, EnrichmentResult};
use crate::types::{MemoryCard, MemoryCardBuilder, MemoryKind, Polarity};
use regex::Regex;

/// Normalize entity names for consistent O(1) lookups.
/// Converts to lowercase and trims whitespace.
fn normalize_entity(entity: &str) -> String {
    entity.trim().to_lowercase()
}

/// A rule for extracting memory cards from text.
#[derive(Debug, Clone)]
pub struct ExtractionRule {
    /// Name of the rule (for debugging).
    pub name: String,
    /// Regex pattern to match.
    pub pattern: Regex,
    /// The kind of memory card to create.
    pub kind: MemoryKind,
    /// The entity to use (supports $1, $2 capture groups).
    pub entity: String,
    /// The slot to use (supports $1, $2 capture groups).
    pub slot: String,
    /// The value template (supports $1, $2 capture groups).
    pub value: String,
    /// Optional polarity for preference rules.
    pub polarity: Option<Polarity>,
}

impl ExtractionRule {
    /// Create a new extraction rule.
    pub fn new(
        name: impl Into<String>,
        pattern: &str,
        kind: MemoryKind,
        entity: impl Into<String>,
        slot: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            name: name.into(),
            pattern: Regex::new(pattern)?,
            kind,
            entity: entity.into(),
            slot: slot.into(),
            value: value.into(),
            polarity: None,
        })
    }

    /// Create a preference rule with polarity.
    pub fn preference(
        name: impl Into<String>,
        pattern: &str,
        entity: impl Into<String>,
        slot: impl Into<String>,
        value: impl Into<String>,
        polarity: Polarity,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            name: name.into(),
            pattern: Regex::new(pattern)?,
            kind: MemoryKind::Preference,
            entity: entity.into(),
            slot: slot.into(),
            value: value.into(),
            polarity: Some(polarity),
        })
    }

    /// Apply the rule to text and return extracted cards.
    fn apply(&self, ctx: &EnrichmentContext) -> Vec<MemoryCard> {
        let mut cards = Vec::new();

        for caps in self.pattern.captures_iter(&ctx.text) {
            // Expand capture groups in entity, slot, value
            let entity = normalize_entity(&self.expand_captures(&self.entity, &caps));
            let slot = self.expand_captures(&self.slot, &caps);
            let value = self.expand_captures(&self.value, &caps).trim().to_string();

            if entity.is_empty() || slot.is_empty() || value.is_empty() {
                continue;
            }

            let mut builder = MemoryCardBuilder::new()
                .kind(self.kind)
                .entity(&entity)
                .slot(&slot)
                .value(&value)
                .source(ctx.frame_id, Some(ctx.uri.clone()))
                .engine("rules", "1.0.0");

            if let Some(polarity) = &self.polarity {
                builder = builder.polarity(*polarity);
            }

            // Build with a placeholder ID (will be assigned by MemoriesTrack)
            if let Ok(card) = builder.build(0) {
                cards.push(card);
            }
        }

        cards
    }

    /// Expand capture group references ($1, $2, etc.) in a template.
    fn expand_captures(&self, template: &str, caps: &regex::Captures) -> String {
        let mut result = template.to_string();
        for i in 0..10 {
            let placeholder = format!("${i}");
            if let Some(m) = caps.get(i) {
                result = result.replace(&placeholder, m.as_str());
            }
        }
        result
    }
}

/// Rules-based enrichment engine.
///
/// This engine uses a collection of regex-based rules to extract
/// structured memory cards from text. Rules can target facts,
/// preferences, events, and other memory types.
#[derive(Debug)]
pub struct RulesEngine {
    rules: Vec<ExtractionRule>,
    version: String,
}

impl Default for RulesEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RulesEngine {
    /// Create a new rules engine with default rules.
    #[must_use]
    pub fn new() -> Self {
        let mut engine = Self {
            rules: Vec::new(),
            version: "1.0.0".to_string(),
        };
        engine.add_default_rules();
        engine.add_third_person_rules();
        engine
    }

    /// Create an empty rules engine (no default rules).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            rules: Vec::new(),
            version: "1.0.0".to_string(),
        }
    }

    /// Add a rule to the engine.
    pub fn add_rule(&mut self, rule: ExtractionRule) {
        self.rules.push(rule);
    }

    /// Add default rules for common patterns.
    fn add_default_rules(&mut self) {
        // Employment facts
        if let Ok(rule) = ExtractionRule::new(
            "employer",
            r"(?i)(?:I work at|I'm employed at|I work for|my employer is|I'm at)\s+([A-Z][a-zA-Z0-9\s&]+?)(?:\.|,|!|\?|$)",
            MemoryKind::Fact,
            "user",
            "employer",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Job title
        if let Ok(rule) = ExtractionRule::new(
            "job_title",
            r"(?i)(?:I am a|I'm a|I work as a|my job is|my role is|my title is)\s+([A-Za-z][a-zA-Z\s]+?)(?:\.|,|!|\?|$| at)",
            MemoryKind::Fact,
            "user",
            "job_title",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Location
        if let Ok(rule) = ExtractionRule::new(
            "location",
            r"(?i)(?:I live in|I'm based in|I'm from|I reside in|my home is in)\s+([A-Z][a-zA-Z\s,]+?)(?:\.|!|\?|$)",
            MemoryKind::Fact,
            "user",
            "location",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Name
        if let Ok(rule) = ExtractionRule::new(
            "name",
            r"(?i)(?:my name is|I'm|call me|I am)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\.|,|!|\?|$)",
            MemoryKind::Profile,
            "user",
            "name",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Age
        if let Ok(rule) = ExtractionRule::new(
            "age",
            r"(?i)(?:I am|I'm)\s+(\d{1,3})\s+(?:years old|yrs old|yo)(?:\.|,|!|\?|$|\s)",
            MemoryKind::Profile,
            "user",
            "age",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Food preferences (positive)
        if let Ok(rule) = ExtractionRule::preference(
            "food_like",
            r"(?i)(?:I (?:really )?(?:love|like|enjoy|prefer|adore))\s+([\w\s]+?)(?:\.|,|!|\?|$)",
            "user",
            "food_preference",
            "$1",
            Polarity::Positive,
        ) {
            self.rules.push(rule);
        }

        // Food preferences (negative)
        if let Ok(rule) = ExtractionRule::preference(
            "food_dislike",
            r"(?i)(?:I (?:really )?(?:hate|dislike|can't stand|don't like|avoid))\s+([\w\s]+?)(?:\.|,|!|\?|$)",
            "user",
            "food_preference",
            "$1",
            Polarity::Negative,
        ) {
            self.rules.push(rule);
        }

        // Allergies
        if let Ok(rule) = ExtractionRule::new(
            "allergy",
            r"(?i)(?:I am|I'm) allergic to\s+([\w\s]+?)(?:\.|,|!|\?|$)",
            MemoryKind::Profile,
            "user",
            "allergy",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Programming language preferences
        if let Ok(rule) = ExtractionRule::preference(
            "programming_language",
            r"(?i)(?:I (?:really )?(?:love|like|enjoy|prefer) (?:programming in|coding in|using|writing))\s+([\w\+\#]+)(?:\.|,|!|\?|$|\s)",
            "user",
            "programming_language",
            "$1",
            Polarity::Positive,
        ) {
            self.rules.push(rule);
        }

        // Hobby/interest
        if let Ok(rule) = ExtractionRule::new(
            "hobby",
            r"(?i)(?:my hobby is|I enjoy|I like to|my favorite hobby is|my favourite hobby is)\s+([\w\s]+?)(?:\.|,|!|\?|$)",
            MemoryKind::Preference,
            "user",
            "hobby",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Pet
        if let Ok(rule) = ExtractionRule::new(
            "pet",
            r"(?i)(?:I have a|my pet is a|I own a)\s+([\w\s]+?)(?:\s+named|\.|,|!|\?|$)",
            MemoryKind::Fact,
            "user",
            "pet",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Pet name
        if let Ok(rule) = ExtractionRule::new(
            "pet_name",
            r"(?i)(?:my (?:pet|dog|cat|bird|fish|hamster)'?s? name is|I have a [\w\s]+ named)\s+([A-Z][a-z]+)(?:\.|,|!|\?|$)",
            MemoryKind::Fact,
            "user",
            "pet_name",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Birthday
        if let Ok(rule) = ExtractionRule::new(
            "birthday",
            r"(?i)(?:my birthday is|I was born on|born on)\s+(\w+\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+\d{4})?)(?:\.|,|!|\?|$)",
            MemoryKind::Profile,
            "user",
            "birthday",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Email
        if let Ok(rule) = ExtractionRule::new(
            "email",
            r"(?i)(?:my email is|email me at|reach me at)\s+([\w\.\-]+@[\w\.\-]+\.\w+)",
            MemoryKind::Profile,
            "user",
            "email",
            "$1",
        ) {
            self.rules.push(rule);
        }

        // Family member mentions
        if let Ok(rule) = ExtractionRule::new(
            "family",
            r"(?i)my\s+(wife|husband|spouse|partner|son|daughter|child|brother|sister|mother|father|mom|dad|grandma|grandmother|grandpa|grandfather)'?s?\s+(?:name is|is named)\s+([A-Z][a-z]+)",
            MemoryKind::Relationship,
            "user",
            "$1",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // Travel/trip events
        if let Ok(rule) = ExtractionRule::new(
            "travel",
            r"(?i)(?:I (?:went|traveled|travelled|visited|am going|will go|am visiting) to)\s+([A-Z][a-zA-Z\s,]+?)(?:\s+(?:last|this|next)|\.|,|!|\?|$)",
            MemoryKind::Event,
            "user",
            "travel",
            "$1",
        ) {
            self.rules.push(rule);
        }
    }

    /// Add rules for third-person statements (e.g., "Alice works at Acme Corp").
    ///
    /// These patterns extract triplets where the subject is a named person
    /// rather than "user" (first-person).
    fn add_third_person_rules(&mut self) {
        // Common name pattern: Capitalized first name, optional middle/last names
        // Matches: "Alice", "John Smith", "Mary Jane Watson"
        let name = r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})";

        // ============================================================
        // EMPLOYMENT PATTERNS
        // ============================================================

        // "Alice works at Acme Corp" / "John is employed at Google"
        if let Ok(rule) = ExtractionRule::new(
            "3p_employer_works_at",
            &format!(
                r"(?i){name}\s+(?:works at|works for|is employed at|is employed by|joined|is at)\s+([A-Z][a-zA-Z0-9\s&]+?)(?:\.|,|!|\?|$|\s+(?:as|in|since))"
            ),
            MemoryKind::Fact,
            "$1",
            "employer",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice is the CEO of Acme Corp" / "Bob is the founder of Startup Inc"
        if let Ok(rule) = ExtractionRule::new(
            "3p_role_at_company",
            &format!(
                r"(?i){name}\s+is\s+(?:the\s+)?([A-Za-z\s]+?)\s+(?:of|at)\s+([A-Z][a-zA-Z0-9\s&]+?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "role",
            "$2 at $3",
        ) {
            self.rules.push(rule);
        }

        // "Alice, CEO of Acme" / "Bob, founder of Startup"
        if let Ok(rule) = ExtractionRule::new(
            "3p_title_appositive",
            &format!(
                r"(?i){name},\s+(?:the\s+)?([A-Za-z\s]+?)\s+(?:of|at)\s+([A-Z][a-zA-Z0-9\s&]+?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "role",
            "$2 at $3",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // LOCATION PATTERNS
        // ============================================================

        // "Alice lives in San Francisco" / "John is based in New York"
        if let Ok(rule) = ExtractionRule::new(
            "3p_location_lives",
            &format!(
                r"(?i){name}\s+(?:lives in|is based in|resides in|is from|comes from|moved to|relocated to)\s+([A-Z][a-zA-Z\s,]+?)(?:\.|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "location",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice is a San Francisco resident" / "John is a New Yorker"
        if let Ok(rule) = ExtractionRule::new(
            "3p_location_resident",
            &format!(
                r"(?i){name}\s+is\s+(?:a\s+)?([A-Z][a-zA-Z\s]+?)(?:\s+resident|\s+native)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "location",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // JOB TITLE / PROFESSION PATTERNS
        // ============================================================

        // "Alice is a software engineer" / "John is an architect"
        if let Ok(rule) = ExtractionRule::new(
            "3p_job_title",
            &format!(
                r"(?i){name}\s+is\s+(?:a|an)\s+([A-Za-z][a-zA-Z\s]+?)(?:\.|,|!|\?|$|\s+(?:at|who|and|with))"
            ),
            MemoryKind::Fact,
            "$1",
            "job_title",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice works as a product manager" / "John works as an engineer"
        if let Ok(rule) = ExtractionRule::new(
            "3p_job_works_as",
            &format!(
                r"(?i){name}\s+works\s+as\s+(?:a|an)\s+([A-Za-z][a-zA-Z\s]+?)(?:\.|,|!|\?|$|\s+(?:at|in|for))"
            ),
            MemoryKind::Fact,
            "$1",
            "job_title",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // RELATIONSHIP PATTERNS
        // ============================================================

        // "Alice is married to Bob" / "John is engaged to Mary"
        if let Ok(rule) = ExtractionRule::new(
            "3p_relationship_married",
            &format!(
                r"(?i){name}\s+is\s+(?:married to|engaged to|dating|in a relationship with|the (?:wife|husband|partner|spouse) of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "spouse",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice and Bob are married" / "John and Mary are dating"
        if let Ok(rule) = ExtractionRule::new(
            "3p_relationship_pair",
            &format!(
                r"(?i){name}\s+and\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+are\s+(?:married|engaged|dating|partners|a couple)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "spouse",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice is Bob's wife" / "John is Mary's husband"
        if let Ok(rule) = ExtractionRule::new(
            "3p_relationship_possessive",
            &format!(
                r"(?i){name}\s+is\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'s\s+(wife|husband|partner|spouse|girlfriend|boyfriend|fiancé|fiancee)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "spouse",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice's husband is Bob" / "John's wife is Mary"
        if let Ok(rule) = ExtractionRule::new(
            "3p_relationship_poss_is",
            &format!(
                r"(?i){name}'s\s+(?:wife|husband|partner|spouse|girlfriend|boyfriend)\s+is\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "spouse",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // Family relationships: "Alice is Bob's mother/sister/etc."
        if let Ok(rule) = ExtractionRule::new(
            "3p_family_member",
            &format!(
                r"(?i){name}\s+is\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'s\s+(mother|father|sister|brother|son|daughter|aunt|uncle|cousin|grandmother|grandfather|grandma|grandpa|mom|dad)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "$3",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice has a brother named Bob"
        if let Ok(rule) = ExtractionRule::new(
            "3p_family_named",
            &format!(
                r"(?i){name}\s+has\s+(?:a\s+)?(brother|sister|son|daughter|mother|father)\s+(?:named|called)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Relationship,
            "$1",
            "$2",
            "$3",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // PREFERENCE PATTERNS
        // ============================================================

        // "Alice loves pizza" / "John enjoys hiking" (positive)
        if let Ok(rule) = ExtractionRule::preference(
            "3p_preference_positive",
            &format!(
                r"(?i){name}\s+(?:loves|likes|enjoys|adores|prefers|is fond of)\s+([\w\s]+?)(?:\.|,|!|\?|$)"
            ),
            "$1",
            "preference",
            "$2",
            Polarity::Positive,
        ) {
            self.rules.push(rule);
        }

        // "Alice hates spiders" / "John dislikes crowds" (negative)
        if let Ok(rule) = ExtractionRule::preference(
            "3p_preference_negative",
            &format!(
                r"(?i){name}\s+(?:hates|dislikes|despises|can't stand|doesn't like|avoids)\s+([\w\s]+?)(?:\.|,|!|\?|$)"
            ),
            "$1",
            "preference",
            "$2",
            Polarity::Negative,
        ) {
            self.rules.push(rule);
        }

        // "Alice's favorite food is sushi"
        if let Ok(rule) = ExtractionRule::new(
            "3p_favorite",
            &format!(
                r"(?i){name}'s\s+(?:favorite|favourite)\s+(\w+)\s+is\s+([\w\s]+?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Preference,
            "$1",
            "favorite_$2",
            "$3",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // EDUCATION PATTERNS
        // ============================================================

        // "Alice studied at MIT" / "John graduated from Harvard"
        if let Ok(rule) = ExtractionRule::new(
            "3p_education_studied",
            &format!(
                r"(?i){name}\s+(?:studied at|graduated from|attends|attended|went to|goes to)\s+([A-Z][a-zA-Z\s]+?(?:University|College|Institute|School|Academy)?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "education",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice has a degree in Computer Science"
        if let Ok(rule) = ExtractionRule::new(
            "3p_education_degree",
            &format!(
                r"(?i){name}\s+has\s+(?:a\s+)?(?:degree|PhD|doctorate|masters?|bachelors?|BA|BS|MS|MBA)\s+in\s+([A-Za-z\s]+?)(?:\.|,|!|\?|$|\s+from)"
            ),
            MemoryKind::Fact,
            "$1",
            "degree",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice majored in Physics"
        if let Ok(rule) = ExtractionRule::new(
            "3p_education_major",
            &format!(
                r"(?i){name}\s+(?:majored in|minored in|studied)\s+([A-Za-z\s]+?)(?:\.|,|!|\?|$|\s+at)"
            ),
            MemoryKind::Fact,
            "$1",
            "field_of_study",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // PROFILE / BIO PATTERNS
        // ============================================================

        // "Alice is 28 years old" / "John is 35"
        if let Ok(rule) = ExtractionRule::new(
            "3p_age",
            &format!(
                r"(?i){name}\s+is\s+(\d{{1,3}})\s*(?:years old|yrs old|yo)?(?:\.|,|!|\?|$|\s)"
            ),
            MemoryKind::Profile,
            "$1",
            "age",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice was born in 1990" / "John was born on March 15"
        if let Ok(rule) = ExtractionRule::new(
            "3p_birthdate",
            &format!(
                r"(?i){name}\s+was\s+born\s+(?:in|on)\s+(\w+(?:\s+\d{{1,2}}(?:st|nd|rd|th)?)?(?:,?\s+\d{{4}})?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Profile,
            "$1",
            "birthdate",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice is from Boston" - birthplace
        if let Ok(rule) = ExtractionRule::new(
            "3p_birthplace",
            &format!(
                r"(?i){name}\s+(?:is|was)\s+(?:originally\s+)?from\s+([A-Z][a-zA-Z\s,]+?)(?:\.|!|\?|$|\s+but)"
            ),
            MemoryKind::Profile,
            "$1",
            "birthplace",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice's email is alice@example.com"
        if let Ok(rule) = ExtractionRule::new(
            "3p_email",
            &format!(r"(?i){name}'s\s+email\s+(?:is|address is)\s+([\w\.\-]+@[\w\.\-]+\.\w+)"),
            MemoryKind::Profile,
            "$1",
            "email",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // HOBBY / INTEREST PATTERNS
        // ============================================================

        // "Alice plays tennis" / "John plays the piano"
        if let Ok(rule) = ExtractionRule::new(
            "3p_hobby_plays",
            &format!(
                r"(?i){name}\s+plays\s+(?:the\s+)?([\w\s]+?)(?:\.|,|!|\?|$|\s+(?:every|on|and))"
            ),
            MemoryKind::Preference,
            "$1",
            "hobby",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice is into photography" / "John is interested in astronomy"
        if let Ok(rule) = ExtractionRule::new(
            "3p_interest",
            &format!(
                r"(?i){name}\s+is\s+(?:into|interested in|passionate about|really into)\s+([\w\s]+?)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Preference,
            "$1",
            "interest",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // PET PATTERNS
        // ============================================================

        // "Alice has a cat named Whiskers"
        if let Ok(rule) = ExtractionRule::new(
            "3p_pet_named",
            &format!(
                r"(?i){name}\s+has\s+(?:a\s+)?(dog|cat|bird|fish|hamster|rabbit|pet)\s+(?:named|called)\s+([A-Z][a-z]+)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "pet_name",
            "$3",
        ) {
            self.rules.push(rule);
        }

        // "Alice's dog is named Max"
        if let Ok(rule) = ExtractionRule::new(
            "3p_pet_poss_named",
            &format!(
                r"(?i){name}'s\s+(dog|cat|bird|fish|hamster|rabbit|pet)\s+is\s+(?:named|called)\s+([A-Z][a-z]+)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "pet_name",
            "$3",
        ) {
            self.rules.push(rule);
        }

        // "Alice owns a golden retriever"
        if let Ok(rule) = ExtractionRule::new(
            "3p_pet_owns",
            &format!(
                r"(?i){name}\s+(?:owns|has)\s+(?:a\s+)?([\w\s]+?)\s+(?:dog|cat|bird|fish|hamster|rabbit)(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Fact,
            "$1",
            "pet",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // ============================================================
        // EVENT PATTERNS
        // ============================================================

        // "Alice visited Paris" / "John traveled to Japan"
        if let Ok(rule) = ExtractionRule::new(
            "3p_travel",
            &format!(
                r"(?i){name}\s+(?:visited|traveled to|travelled to|went to|is going to|will visit)\s+([A-Z][a-zA-Z\s,]+?)(?:\s+(?:last|this|next)|\.|,|!|\?|$)"
            ),
            MemoryKind::Event,
            "$1",
            "travel",
            "$2",
        ) {
            self.rules.push(rule);
        }

        // "Alice started at Google in 2020"
        if let Ok(rule) = ExtractionRule::new(
            "3p_career_event",
            &format!(
                r"(?i){name}\s+(?:started|joined|left|quit|founded)\s+(?:at\s+)?([A-Z][a-zA-Z0-9\s&]+?)(?:\s+in\s+\d{{4}})?(?:\.|,|!|\?|$)"
            ),
            MemoryKind::Event,
            "$1",
            "career_event",
            "$2",
        ) {
            self.rules.push(rule);
        }
    }

    /// Get the number of rules in this engine.
    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

impl EnrichmentEngine for RulesEngine {
    fn kind(&self) -> &'static str {
        "rules"
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn enrich(&self, ctx: &EnrichmentContext) -> EnrichmentResult {
        let mut all_cards = Vec::new();

        for rule in &self.rules {
            let cards = rule.apply(ctx);
            all_cards.extend(cards);
        }

        EnrichmentResult::success(all_cards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_context(text: &str) -> EnrichmentContext {
        EnrichmentContext::new(
            1,
            "mv2://test/msg-1".to_string(),
            text.to_string(),
            None,
            1700000000,
            None,
        )
    }

    #[test]
    fn test_rules_engine_default() {
        let engine = RulesEngine::new();
        assert!(engine.rule_count() > 0);
        assert_eq!(engine.kind(), "rules");
        assert_eq!(engine.version(), "1.0.0");
    }

    #[test]
    fn test_extract_employer() {
        let engine = RulesEngine::new();
        let ctx = test_context("Hi, I work at Anthropic.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        // Find the first-person employer card
        let card = result
            .cards
            .iter()
            .find(|c| c.entity == "user" && c.slot == "employer")
            .unwrap();
        assert_eq!(card.value, "Anthropic");
    }

    #[test]
    fn test_extract_location() {
        let engine = RulesEngine::new();
        let ctx = test_context("I live in San Francisco.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        // Find the first-person location card
        let card = result
            .cards
            .iter()
            .find(|c| c.entity == "user" && c.slot == "location")
            .unwrap();
        assert_eq!(card.value, "San Francisco");
    }

    #[test]
    fn test_extract_preference_positive() {
        let engine = RulesEngine::new();
        let ctx = test_context("I really love sushi.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].kind, MemoryKind::Preference);
        assert_eq!(result.cards[0].slot, "food_preference");
        assert_eq!(result.cards[0].value, "sushi");
        assert_eq!(result.cards[0].polarity, Some(Polarity::Positive));
    }

    #[test]
    fn test_extract_preference_negative() {
        let engine = RulesEngine::new();
        let ctx = test_context("I really hate cilantro.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].polarity, Some(Polarity::Negative));
        assert_eq!(result.cards[0].value, "cilantro");
    }

    #[test]
    fn test_multiple_extractions() {
        let engine = RulesEngine::new();
        let ctx = test_context("I work at Google. I live in Mountain View. I love programming.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert!(result.cards.len() >= 2);
    }

    #[test]
    fn test_no_matches() {
        let engine = RulesEngine::new();
        let ctx = test_context("The weather is nice today.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert!(result.cards.is_empty());
    }

    #[test]
    fn test_extract_name() {
        let engine = RulesEngine::new();
        let ctx = test_context("My name is John Smith.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].slot, "name");
        assert_eq!(result.cards[0].value, "John Smith");
    }

    #[test]
    fn test_extract_pet() {
        let engine = RulesEngine::new();
        let ctx = test_context("I have a golden retriever named Max.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        // Should extract both "pet" and "pet_name"
        let pet_card = result.cards.iter().find(|c| c.slot == "pet");
        let name_card = result.cards.iter().find(|c| c.slot == "pet_name");
        assert!(pet_card.is_some());
        assert!(name_card.is_some());
        assert_eq!(name_card.unwrap().value, "Max");
    }

    #[test]
    fn test_custom_rule() {
        let mut engine = RulesEngine::empty();
        let rule = ExtractionRule::new(
            "custom",
            r"(?i)favorite color is\s+(\w+)",
            MemoryKind::Preference,
            "user",
            "favorite_color",
            "$1",
        )
        .unwrap();
        engine.add_rule(rule);

        let ctx = test_context("My favorite color is blue.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].slot, "favorite_color");
        assert_eq!(result.cards[0].value, "blue");
    }

    // ========================================================
    // THIRD-PERSON PATTERN TESTS
    // ========================================================

    #[test]
    fn test_3p_employer() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice works at Acme Corp.");
        let result = engine.enrich(&ctx);

        assert!(result.success);
        let card = result.cards.iter().find(|c| c.slot == "employer").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "Acme Corp");
    }

    #[test]
    fn test_3p_employer_variations() {
        let engine = RulesEngine::new();

        // "is employed at"
        let ctx = test_context("John Smith is employed at Google.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "employer").unwrap();
        assert_eq!(card.entity, "john smith");
        assert_eq!(card.value, "Google");

        // "joined"
        let ctx = test_context("Mary joined Microsoft.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "employer").unwrap();
        assert_eq!(card.entity, "mary");
        assert_eq!(card.value, "Microsoft");
    }

    #[test]
    fn test_3p_location() {
        let engine = RulesEngine::new();

        // "lives in"
        let ctx = test_context("Alice lives in San Francisco.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "location").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "San Francisco");

        // "is based in"
        let ctx = test_context("Bob is based in New York City.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "location").unwrap();
        assert_eq!(card.entity, "bob");
        assert!(card.value.contains("New York"));
    }

    #[test]
    fn test_3p_job_title() {
        let engine = RulesEngine::new();

        // "is a"
        let ctx = test_context("Alice is a software engineer.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "job_title").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "software engineer");

        // "works as"
        let ctx = test_context("John works as a product manager.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "job_title").unwrap();
        assert_eq!(card.entity, "john");
        assert_eq!(card.value, "product manager");
    }

    #[test]
    fn test_3p_relationship_married() {
        let engine = RulesEngine::new();

        // "is married to"
        let ctx = test_context("Alice is married to Bob.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "spouse").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "Bob");

        // "and are married"
        let ctx = test_context("John and Mary are married.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "spouse").unwrap();
        assert_eq!(card.entity, "john");
        assert_eq!(card.value, "Mary");
    }

    #[test]
    fn test_3p_preference_positive() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice loves sushi.");
        let result = engine.enrich(&ctx);

        let card = result
            .cards
            .iter()
            .find(|c| c.slot == "preference")
            .unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "sushi");
        assert_eq!(card.polarity, Some(Polarity::Positive));
    }

    #[test]
    fn test_3p_preference_negative() {
        let engine = RulesEngine::new();
        let ctx = test_context("Bob hates spiders.");
        let result = engine.enrich(&ctx);

        let card = result
            .cards
            .iter()
            .find(|c| c.slot == "preference")
            .unwrap();
        assert_eq!(card.entity, "bob");
        assert_eq!(card.value, "spiders");
        assert_eq!(card.polarity, Some(Polarity::Negative));
    }

    #[test]
    fn test_3p_education() {
        let engine = RulesEngine::new();

        // "graduated from"
        let ctx = test_context("Alice graduated from MIT.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "education").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "MIT");

        // "studied at"
        let ctx = test_context("John studied at Stanford University.");
        let result = engine.enrich(&ctx);
        let card = result.cards.iter().find(|c| c.slot == "education").unwrap();
        assert_eq!(card.entity, "john");
        assert!(card.value.contains("Stanford"));
    }

    #[test]
    fn test_3p_age() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice is 28 years old.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "age").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "28");
    }

    #[test]
    fn test_3p_travel() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice visited Paris.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "travel").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "Paris");
    }

    #[test]
    fn test_3p_hobby() {
        let engine = RulesEngine::new();
        let ctx = test_context("Bob plays tennis.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "hobby").unwrap();
        assert_eq!(card.entity, "bob");
        assert_eq!(card.value, "tennis");
    }

    #[test]
    fn test_3p_multiple_extractions() {
        let engine = RulesEngine::new();
        let ctx = test_context(
            "Alice works at Google. She lives in Mountain View. Bob is a doctor in Seattle.",
        );
        let result = engine.enrich(&ctx);

        assert!(result.success);
        // Should extract multiple facts about Alice and Bob
        let alice_employer = result
            .cards
            .iter()
            .find(|c| c.entity == "alice" && c.slot == "employer");
        let bob_job = result
            .cards
            .iter()
            .find(|c| c.entity == "bob" && c.slot == "job_title");

        assert!(alice_employer.is_some());
        assert!(bob_job.is_some());
    }

    #[test]
    fn test_entity_normalization() {
        let engine = RulesEngine::new();

        // Entities should be normalized to lowercase for consistent O(1) lookups
        let ctx = test_context("ALICE SMITH works at Acme.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "employer");
        assert!(card.is_some());
        // Entity should be lowercase
        assert_eq!(card.unwrap().entity, "alice smith");
    }

    #[test]
    fn test_3p_pet() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice has a cat named Whiskers.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "pet_name").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "Whiskers");
    }

    #[test]
    fn test_3p_family() {
        let engine = RulesEngine::new();
        let ctx = test_context("Alice has a brother named Bob.");
        let result = engine.enrich(&ctx);

        let card = result.cards.iter().find(|c| c.slot == "brother").unwrap();
        assert_eq!(card.entity, "alice");
        assert_eq!(card.value, "Bob");
    }
}
