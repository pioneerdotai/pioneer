//! Schema types for predicate validation and type checking.
//!
//! This module provides typed schemas for predicates (slots) that enable:
//! - Domain validation (which entity kinds can have this predicate)
//! - Range validation (what type of value is expected)
//! - Cardinality enforcement (single vs. multiple values)
//! - Inverse relationship tracking

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::logic_mesh::EntityKind;

/// Unique identifier for a predicate in the schema.
pub type PredicateId = String;

/// The type of value a predicate can hold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ValueType {
    /// Free-form string value.
    String,
    /// Numeric value (integer or float).
    Number,
    /// Unix timestamp or ISO 8601 date.
    DateTime,
    /// Boolean value.
    Boolean,
    /// Reference to another entity of a specific kind.
    EntityRef {
        /// The kind of entity being referenced.
        kind: EntityKind,
    },
    /// Controlled vocabulary (one of a fixed set of values).
    Enum {
        /// The allowed values.
        values: Vec<String>,
    },
    /// Any value type (no validation).
    Any,
}

impl Default for ValueType {
    fn default() -> Self {
        Self::String
    }
}

impl ValueType {
    /// Check if a value matches this type.
    #[must_use]
    pub fn matches(&self, value: &str) -> bool {
        match self {
            Self::String | Self::Any => true,
            Self::Number => value.parse::<f64>().is_ok(),
            Self::DateTime => {
                // Check for Unix timestamp or ISO 8601
                value.parse::<i64>().is_ok() || value.contains('T') || value.contains('-')
            }
            Self::Boolean => matches!(
                value.to_lowercase().as_str(),
                "true" | "false" | "yes" | "no" | "1" | "0"
            ),
            Self::EntityRef { .. } => !value.is_empty(), // Just check non-empty for now
            Self::Enum { values } => values.iter().any(|v| v.eq_ignore_ascii_case(value)),
        }
    }

    /// Get a human-readable description of this type.
    #[must_use]
    pub fn description(&self) -> String {
        match self {
            Self::String => "string".to_string(),
            Self::Number => "number".to_string(),
            Self::DateTime => "datetime".to_string(),
            Self::Boolean => "boolean".to_string(),
            Self::EntityRef { kind } => format!("ref:{}", kind.as_str()),
            Self::Enum { values } => format!("enum[{}]", values.join("|")),
            Self::Any => "any".to_string(),
        }
    }
}

/// Cardinality of a predicate (single or multiple values allowed).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// Only one value per entity (e.g., employer, birthdate).
    #[default]
    Single,
    /// Multiple values allowed (e.g., hobbies, skills).
    Multiple,
}

/// Schema definition for a predicate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateSchema {
    /// Unique identifier for this predicate.
    pub id: PredicateId,

    /// Human-readable name.
    pub name: String,

    /// Optional description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Entity kinds that can have this predicate (empty = any).
    #[serde(default)]
    pub domain: Vec<EntityKind>,

    /// Expected value type.
    #[serde(default)]
    pub range: ValueType,

    /// Whether multiple values are allowed.
    #[serde(default)]
    pub cardinality: Cardinality,

    /// Inverse predicate (e.g., "employer" <-> "employee").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inverse: Option<PredicateId>,

    /// Whether this is a built-in schema.
    #[serde(default)]
    pub builtin: bool,
}

impl PredicateSchema {
    /// Create a new predicate schema.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            id: id.clone(),
            name: name.into(),
            description: None,
            domain: Vec::new(),
            range: ValueType::String,
            cardinality: Cardinality::Single,
            inverse: None,
            builtin: false,
        }
    }

    /// Set the domain (entity kinds).
    #[must_use]
    pub fn with_domain(mut self, kinds: Vec<EntityKind>) -> Self {
        self.domain = kinds;
        self
    }

    /// Set the range (value type).
    #[must_use]
    pub fn with_range(mut self, range: ValueType) -> Self {
        self.range = range;
        self
    }

    /// Set cardinality to multiple.
    #[must_use]
    pub fn multiple(mut self) -> Self {
        self.cardinality = Cardinality::Multiple;
        self
    }

    /// Set the inverse predicate.
    pub fn with_inverse(mut self, inverse: impl Into<String>) -> Self {
        self.inverse = Some(inverse.into());
        self
    }

    /// Mark as built-in.
    #[must_use]
    pub fn builtin(mut self) -> Self {
        self.builtin = true;
        self
    }

    /// Check if an entity kind is in the domain.
    #[must_use]
    pub fn allows_entity(&self, kind: EntityKind) -> bool {
        self.domain.is_empty() || self.domain.contains(&kind)
    }

    /// Validate a value against this schema.
    pub fn validate_value(&self, value: &str) -> Result<(), SchemaError> {
        if !self.range.matches(value) {
            return Err(SchemaError::InvalidRange {
                predicate: self.id.clone(),
                expected: self.range.description(),
                got: value.to_string(),
            });
        }
        Ok(())
    }
}

/// Error type for schema validation.
#[derive(Debug, Clone)]
pub enum SchemaError {
    /// Entity kind not allowed for this predicate.
    InvalidDomain {
        predicate: String,
        entity_kind: String,
        allowed: Vec<String>,
    },
    /// Value type doesn't match expected range.
    InvalidRange {
        predicate: String,
        expected: String,
        got: String,
    },
    /// Cardinality violation (single value already exists).
    CardinalityViolation { predicate: String, entity: String },
    /// Unknown predicate.
    UnknownPredicate(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDomain {
                predicate,
                entity_kind,
                allowed,
            } => {
                write!(
                    f,
                    "predicate '{predicate}' not allowed for entity kind '{entity_kind}' (allowed: {allowed:?})"
                )
            }
            Self::InvalidRange {
                predicate,
                expected,
                got,
            } => {
                write!(
                    f,
                    "invalid value for '{predicate}': expected {expected}, got '{got}'"
                )
            }
            Self::CardinalityViolation { predicate, entity } => {
                write!(
                    f,
                    "cardinality violation: '{entity}' already has a value for '{predicate}'"
                )
            }
            Self::UnknownPredicate(p) => write!(f, "unknown predicate: '{p}'"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Registry of predicate schemas with built-in defaults.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaRegistry {
    /// All registered schemas.
    schemas: HashMap<PredicateId, PredicateSchema>,
    /// Whether to enforce strict validation.
    #[serde(default)]
    strict: bool,
}

impl SchemaRegistry {
    /// Create a new registry with built-in schemas.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Self {
            schemas: HashMap::new(),
            strict: false,
        };
        registry.register_builtin_schemas();
        registry
    }

    /// Create an empty registry without built-in schemas.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            schemas: HashMap::new(),
            strict: false,
        }
    }

    /// Enable strict validation (unknown predicates are rejected).
    #[must_use]
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Register built-in schemas for common predicates.
    fn register_builtin_schemas(&mut self) {
        // Employment predicates
        self.register(
            PredicateSchema::new("employer", "Employer")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::String)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("workplace", "Workplace")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::String)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("job_title", "Job Title")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::String)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("occupation", "Occupation")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::String)
                .builtin(),
        );

        // Location predicates
        self.register(
            PredicateSchema::new("location", "Location")
                .with_range(ValueType::String)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("city", "City")
                .with_range(ValueType::String)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("country", "Country")
                .with_range(ValueType::String)
                .builtin(),
        );

        // Relationship predicates
        self.register(
            PredicateSchema::new("spouse", "Spouse")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::EntityRef {
                    kind: EntityKind::Person,
                })
                .with_inverse("spouse")
                .builtin(),
        );
        self.register(
            PredicateSchema::new("manager", "Manager")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::EntityRef {
                    kind: EntityKind::Person,
                })
                .with_inverse("reports")
                .builtin(),
        );

        // Preference predicates (multiple allowed)
        self.register(
            PredicateSchema::new("likes", "Likes")
                .with_domain(vec![EntityKind::Person])
                .multiple()
                .builtin(),
        );
        self.register(
            PredicateSchema::new("dislikes", "Dislikes")
                .with_domain(vec![EntityKind::Person])
                .multiple()
                .builtin(),
        );
        self.register(
            PredicateSchema::new("preference", "Preference")
                .multiple()
                .builtin(),
        );
        self.register(
            PredicateSchema::new("hobby", "Hobby")
                .with_domain(vec![EntityKind::Person])
                .multiple()
                .builtin(),
        );

        // Personal info predicates
        self.register(
            PredicateSchema::new("age", "Age")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::Number)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("birthday", "Birthday")
                .with_domain(vec![EntityKind::Person])
                .with_range(ValueType::DateTime)
                .builtin(),
        );
        self.register(
            PredicateSchema::new("education", "Education")
                .with_domain(vec![EntityKind::Person])
                .multiple()
                .builtin(),
        );
        self.register(
            PredicateSchema::new("email", "Email")
                .with_range(ValueType::String)
                .builtin(),
        );

        // Pet predicate
        self.register(
            PredicateSchema::new("pet", "Pet")
                .with_domain(vec![EntityKind::Person])
                .multiple()
                .builtin(),
        );
    }

    /// Register a schema.
    pub fn register(&mut self, schema: PredicateSchema) {
        self.schemas.insert(schema.id.clone(), schema);
    }

    /// Get a schema by predicate ID.
    #[must_use]
    pub fn get(&self, predicate: &str) -> Option<&PredicateSchema> {
        self.schemas.get(predicate)
    }

    /// Check if a predicate is known.
    #[must_use]
    pub fn contains(&self, predicate: &str) -> bool {
        self.schemas.contains_key(predicate)
    }

    /// Get all schema entries.
    pub fn all(&self) -> impl Iterator<Item = &PredicateSchema> {
        self.schemas.values()
    }

    /// Validate a triplet against the schema.
    pub fn validate(
        &self,
        predicate: &str,
        value: &str,
        entity_kind: Option<EntityKind>,
    ) -> Result<(), SchemaError> {
        let schema = if let Some(s) = self.schemas.get(predicate) {
            s
        } else {
            if self.strict {
                return Err(SchemaError::UnknownPredicate(predicate.to_string()));
            }
            return Ok(()); // Allow unknown predicates in non-strict mode
        };

        // Validate domain
        if let Some(kind) = entity_kind {
            if !schema.allows_entity(kind) {
                return Err(SchemaError::InvalidDomain {
                    predicate: predicate.to_string(),
                    entity_kind: kind.as_str().to_string(),
                    allowed: schema
                        .domain
                        .iter()
                        .map(|k| k.as_str().to_string())
                        .collect(),
                });
            }
        }

        // Validate range
        schema.validate_value(value)?;

        Ok(())
    }

    /// Infer a schema from existing memory cards.
    #[must_use]
    pub fn infer_from_values(&self, predicate: &str, values: &[&str]) -> PredicateSchema {
        let mut schema = PredicateSchema::new(predicate, predicate);

        // Try to infer type from values
        let all_numeric = values.iter().all(|v| v.parse::<f64>().is_ok());
        let all_datetime = values
            .iter()
            .all(|v| v.parse::<i64>().is_ok() || v.contains('T'));
        let all_boolean = values.iter().all(|v| {
            matches!(
                v.to_lowercase().as_str(),
                "true" | "false" | "yes" | "no" | "1" | "0"
            )
        });

        if all_numeric && !values.is_empty() {
            schema.range = ValueType::Number;
        } else if all_datetime && !values.is_empty() {
            schema.range = ValueType::DateTime;
        } else if all_boolean && !values.is_empty() {
            schema.range = ValueType::Boolean;
        }

        // Check cardinality (if more than one unique value for same entity)
        let unique_values: std::collections::HashSet<_> = values.iter().collect();
        if unique_values.len() > 1 {
            schema.cardinality = Cardinality::Multiple;
        }

        schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches("hello"));
        assert!(ValueType::Number.matches("42"));
        assert!(ValueType::Number.matches("3.14"));
        assert!(!ValueType::Number.matches("abc"));
        assert!(ValueType::Boolean.matches("true"));
        assert!(ValueType::Boolean.matches("false"));
        assert!(!ValueType::Boolean.matches("maybe"));
        assert!(
            ValueType::Enum {
                values: vec!["a".to_string(), "b".to_string()]
            }
            .matches("A")
        );
        assert!(
            !ValueType::Enum {
                values: vec!["a".to_string(), "b".to_string()]
            }
            .matches("c")
        );
    }

    #[test]
    fn test_predicate_schema_validation() {
        let schema = PredicateSchema::new("age", "Age")
            .with_domain(vec![EntityKind::Person])
            .with_range(ValueType::Number);

        assert!(schema.validate_value("25").is_ok());
        assert!(schema.validate_value("abc").is_err());
        assert!(schema.allows_entity(EntityKind::Person));
        assert!(!schema.allows_entity(EntityKind::Organization));
    }

    #[test]
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();

        // Built-in schemas should exist
        assert!(registry.contains("employer"));
        assert!(registry.contains("location"));
        assert!(registry.contains("age"));

        // Validate against built-in
        assert!(
            registry
                .validate("age", "25", Some(EntityKind::Person))
                .is_ok()
        );
        assert!(
            registry
                .validate("age", "abc", Some(EntityKind::Person))
                .is_err()
        );
    }

    #[test]
    fn test_schema_registry_strict() {
        let registry = SchemaRegistry::new().strict();

        // Unknown predicate should fail in strict mode
        assert!(registry.validate("unknown_pred", "value", None).is_err());
    }

    #[test]
    fn test_schema_inference() {
        let registry = SchemaRegistry::new();

        let values = vec!["25", "30", "45"];
        let schema = registry.infer_from_values("age", &values);
        assert_eq!(schema.range, ValueType::Number);

        let values = vec!["true", "false", "true"];
        let schema = registry.infer_from_values("active", &values);
        assert_eq!(schema.range, ValueType::Boolean);
    }
}
