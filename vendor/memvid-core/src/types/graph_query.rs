//! Graph-aware query types for hybrid retrieval.
//!
//! Enables combining graph traversal with vector similarity for relational queries.

use serde::{Deserialize, Serialize};

use super::common::FrameId;

/// A triple pattern for graph matching.
/// Variables start with `?`, literals are exact matches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TriplePattern {
    /// Subject: entity name or `?var`
    pub subject: PatternTerm,
    /// Predicate: slot/relationship name or `?var`
    pub predicate: PatternTerm,
    /// Object: value or entity or `?var`
    pub object: PatternTerm,
}

impl TriplePattern {
    /// Create a new triple pattern.
    #[must_use]
    pub fn new(subject: PatternTerm, predicate: PatternTerm, object: PatternTerm) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }

    /// Create a pattern matching entity:slot = value
    #[must_use]
    pub fn entity_slot_value(entity: &str, slot: &str, value: &str) -> Self {
        Self {
            subject: PatternTerm::Literal(entity.to_lowercase()),
            predicate: PatternTerm::Literal(slot.to_lowercase()),
            object: PatternTerm::Literal(value.to_string()),
        }
    }

    /// Create a pattern matching entity:slot = ?var (any value)
    #[must_use]
    pub fn entity_slot_any(entity: &str, slot: &str, var: &str) -> Self {
        Self {
            subject: PatternTerm::Literal(entity.to_lowercase()),
            predicate: PatternTerm::Literal(slot.to_lowercase()),
            object: PatternTerm::Variable(var.to_string()),
        }
    }

    /// Create a pattern matching ?entity:slot = value (find entities with this value)
    #[must_use]
    pub fn any_slot_value(var: &str, slot: &str, value: &str) -> Self {
        Self {
            subject: PatternTerm::Variable(var.to_string()),
            predicate: PatternTerm::Literal(slot.to_lowercase()),
            object: PatternTerm::Literal(value.to_string()),
        }
    }
}

/// A term in a triple pattern - either a variable or literal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PatternTerm {
    /// Variable binding (e.g., `?user`, `?food`)
    Variable(String),
    /// Literal value (e.g., "alice", "employer", "Anthropic")
    Literal(String),
}

impl PatternTerm {
    /// Check if this term is a variable.
    #[must_use]
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(_))
    }

    /// Get the variable name if this is a variable.
    #[must_use]
    pub fn variable_name(&self) -> Option<&str> {
        match self {
            Self::Variable(name) => Some(name),
            Self::Literal(_) => None,
        }
    }

    /// Get the literal value if this is a literal.
    #[must_use]
    pub fn literal_value(&self) -> Option<&str> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Variable(_) => None,
        }
    }
}

/// A graph pattern for filtering - conjunction of triple patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphPattern {
    /// Triple patterns to match (all must match - AND semantics)
    pub triples: Vec<TriplePattern>,
}

impl GraphPattern {
    /// Create an empty pattern.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a triple pattern.
    pub fn add(&mut self, pattern: TriplePattern) {
        self.triples.push(pattern);
    }

    /// Create from a single triple pattern.
    #[must_use]
    pub fn single(pattern: TriplePattern) -> Self {
        Self {
            triples: vec![pattern],
        }
    }

    /// Check if the pattern is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.triples.is_empty()
    }

    /// Get all variables used in this pattern.
    #[must_use]
    pub fn variables(&self) -> Vec<&str> {
        let mut vars = Vec::new();
        for triple in &self.triples {
            if let Some(v) = triple.subject.variable_name() {
                if !vars.contains(&v) {
                    vars.push(v);
                }
            }
            if let Some(v) = triple.predicate.variable_name() {
                if !vars.contains(&v) {
                    vars.push(v);
                }
            }
            if let Some(v) = triple.object.variable_name() {
                if !vars.contains(&v) {
                    vars.push(v);
                }
            }
        }
        vars
    }
}

/// Query plan for graph-aware retrieval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryPlan {
    /// Pure vector similarity (fallback when no graph patterns detected)
    VectorOnly {
        /// Query embedding
        query_embedding: Option<Vec<f32>>,
        /// Query text for lexical search
        query_text: Option<String>,
        /// Number of results
        top_k: usize,
    },

    /// Pure graph traversal (for relational queries)
    GraphOnly {
        /// Graph pattern to match
        pattern: GraphPattern,
        /// Maximum results
        limit: usize,
    },

    /// Hybrid: Graph filter + Vector rank
    Hybrid {
        /// First: Graph pattern to get candidate entities/frames
        graph_filter: GraphPattern,
        /// Then: Vector similarity to rank within candidates
        query_embedding: Option<Vec<f32>>,
        /// Query text for lexical boosting
        query_text: Option<String>,
        /// Number of final results
        top_k: usize,
    },
}

impl QueryPlan {
    /// Create a vector-only plan.
    #[must_use]
    pub fn vector_only(
        query_text: Option<String>,
        query_embedding: Option<Vec<f32>>,
        top_k: usize,
    ) -> Self {
        Self::VectorOnly {
            query_embedding,
            query_text,
            top_k,
        }
    }

    /// Create a graph-only plan.
    #[must_use]
    pub fn graph_only(pattern: GraphPattern, limit: usize) -> Self {
        Self::GraphOnly { pattern, limit }
    }

    /// Create a hybrid plan.
    #[must_use]
    pub fn hybrid(
        graph_filter: GraphPattern,
        query_text: Option<String>,
        query_embedding: Option<Vec<f32>>,
        top_k: usize,
    ) -> Self {
        Self::Hybrid {
            graph_filter,
            query_embedding,
            query_text,
            top_k,
        }
    }

    /// Check if this plan uses graph patterns.
    #[must_use]
    pub fn uses_graph(&self) -> bool {
        matches!(self, Self::GraphOnly { .. } | Self::Hybrid { .. })
    }

    /// Check if this plan uses vector search.
    #[must_use]
    pub fn uses_vector(&self) -> bool {
        matches!(self, Self::VectorOnly { .. } | Self::Hybrid { .. })
    }
}

/// Result of graph pattern matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMatchResult {
    /// Matched entity name
    pub entity: String,
    /// Frame IDs where the match was found
    pub frame_ids: Vec<FrameId>,
    /// Variable bindings for this match
    pub bindings: std::collections::HashMap<String, String>,
    /// Confidence score (0.0-1.0)
    pub confidence: f32,
}

impl GraphMatchResult {
    /// Create a new match result.
    #[must_use]
    pub fn new(entity: String, frame_ids: Vec<FrameId>, confidence: f32) -> Self {
        Self {
            entity,
            frame_ids,
            bindings: std::collections::HashMap::new(),
            confidence,
        }
    }

    /// Add a variable binding.
    pub fn bind(&mut self, var: &str, value: String) {
        self.bindings.insert(var.to_string(), value);
    }
}

/// Result of hybrid search combining graph and vector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchHit {
    /// Frame ID
    pub frame_id: FrameId,
    /// Combined score (graph + vector)
    pub score: f32,
    /// Graph pattern match score (0.0-1.0)
    pub graph_score: f32,
    /// Vector similarity score (0.0-1.0)
    pub vector_score: f32,
    /// Entity that matched the graph pattern
    pub matched_entity: Option<String>,
    /// Frame content preview
    pub preview: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_triple_pattern_creation() {
        let pattern = TriplePattern::entity_slot_value("alice", "employer", "Anthropic");
        assert_eq!(pattern.subject, PatternTerm::Literal("alice".to_string()));
        assert_eq!(
            pattern.predicate,
            PatternTerm::Literal("employer".to_string())
        );
        assert_eq!(
            pattern.object,
            PatternTerm::Literal("Anthropic".to_string())
        );
    }

    #[test]
    fn test_graph_pattern_variables() {
        let mut pattern = GraphPattern::new();
        pattern.add(TriplePattern::any_slot_value(
            "user",
            "location",
            "San Francisco",
        ));
        pattern.add(TriplePattern::entity_slot_any(
            "?user", "employer", "company",
        ));

        let vars = pattern.variables();
        assert!(vars.contains(&"user"));
        assert!(vars.contains(&"company"));
    }

    #[test]
    fn test_query_plan_types() {
        let vector_plan = QueryPlan::vector_only(Some("test".into()), None, 10);
        assert!(!vector_plan.uses_graph());
        assert!(vector_plan.uses_vector());

        let graph_plan = QueryPlan::graph_only(GraphPattern::new(), 10);
        assert!(graph_plan.uses_graph());
        assert!(!graph_plan.uses_vector());

        let hybrid_plan = QueryPlan::hybrid(GraphPattern::new(), Some("test".into()), None, 10);
        assert!(hybrid_plan.uses_graph());
        assert!(hybrid_plan.uses_vector());
    }
}
