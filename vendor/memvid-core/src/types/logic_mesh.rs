//! Logic-Mesh on-disk graph structure for entity and relationship tracking.
//!
//! Logic-Mesh is a graph track inside `.mv2` that detects entities and relationships
//! during ingestion, allowing Memvid to follow facts instead of guessing with vectors.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use super::common::FrameId;
use crate::{MemvidError, Result};

/// Magic bytes for Logic-Mesh blob.
pub const LOGIC_MESH_MAGIC: &[u8; 4] = b"MVLM";

/// Current schema version.
pub const LOGIC_MESH_VERSION: u16 = 1;

/// Maximum nodes allowed (`DoS` prevention).
pub const MAX_MESH_NODES: usize = 1_000_000;

/// Maximum edges allowed (`DoS` prevention).
pub const MAX_MESH_EDGES: usize = 5_000_000;

/// A node in the logic mesh representing an entity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshNode {
    /// Unique node ID (deterministic: hash of `canonical_name` + kind).
    pub id: u64,
    /// Canonical entity name (lowercased, normalized).
    pub canonical_name: String,
    /// Display name (original casing).
    pub display_name: String,
    /// Entity kind.
    pub kind: EntityKind,
    /// Confidence score from NER (0.0–1.0), stored as u8 (0-100).
    pub confidence: u8,
    /// Frame IDs where this entity appears.
    pub frame_ids: Vec<FrameId>,
    /// Byte spans within frames: (`frame_id`, `byte_start`, `byte_len`).
    pub mentions: Vec<(FrameId, u32, u16)>,
}

impl MeshNode {
    /// Create a new mesh node with computed ID.
    #[must_use]
    pub fn new(
        canonical_name: String,
        display_name: String,
        kind: EntityKind,
        confidence: f32,
        frame_id: FrameId,
        byte_start: u32,
        byte_len: u16,
    ) -> Self {
        let id = compute_node_id(&canonical_name, kind);
        #[allow(clippy::cast_possible_truncation)]
        let confidence = (confidence * 100.0).min(100.0) as u8;

        Self {
            id,
            canonical_name,
            display_name,
            kind,
            confidence,
            frame_ids: vec![frame_id],
            mentions: vec![(frame_id, byte_start, byte_len)],
        }
    }

    /// Get confidence as f32 (0.0-1.0).
    #[must_use]
    pub fn confidence_f32(&self) -> f32 {
        f32::from(self.confidence) / 100.0
    }
}

/// Entity classification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum EntityKind {
    Person = 0,
    Organization = 1,
    Project = 2,
    Email = 3,
    Date = 4,
    Location = 5,
    Product = 6,
    Event = 7,
    Money = 8,
    Url = 9,
    Other = 255,
}

impl EntityKind {
    /// Parse entity kind from NER label string.
    #[must_use]
    pub fn from_label(label: &str) -> Self {
        match label.to_lowercase().as_str() {
            "person" | "per" => Self::Person,
            "organization" | "org" | "company" => Self::Organization,
            "project" | "product" => Self::Project,
            "email" => Self::Email,
            "date" | "time" => Self::Date,
            "location" | "loc" | "gpe" => Self::Location,
            "money" | "currency" => Self::Money,
            "url" | "link" => Self::Url,
            "event" => Self::Event,
            _ => Self::Other,
        }
    }

    /// Get display name for this entity kind.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Organization => "organization",
            Self::Project => "project",
            Self::Email => "email",
            Self::Date => "date",
            Self::Location => "location",
            Self::Product => "product",
            Self::Event => "event",
            Self::Money => "money",
            Self::Url => "url",
            Self::Other => "other",
        }
    }
}

/// A directed edge in the logic mesh representing a relationship.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshEdge {
    /// Source node ID.
    pub from_node: u64,
    /// Target node ID.
    pub to_node: u64,
    /// Relationship type.
    pub link: LinkType,
    /// Confidence score (0-100).
    pub confidence: u8,
    /// Frame ID where relationship was detected.
    pub frame_id: FrameId,
}

impl MeshEdge {
    /// Create a new edge.
    #[must_use]
    pub fn new(
        from_node: u64,
        to_node: u64,
        link: LinkType,
        confidence: f32,
        frame_id: FrameId,
    ) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let confidence = (confidence * 100.0).min(100.0) as u8;

        Self {
            from_node,
            to_node,
            link,
            confidence,
            frame_id,
        }
    }

    /// Get confidence as f32 (0.0-1.0).
    #[must_use]
    pub fn confidence_f32(&self) -> f32 {
        f32::from(self.confidence) / 100.0
    }
}

/// Relationship types between entities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LinkType {
    Manager,
    Member,
    Owner,
    Author,
    Email,
    Deadline,
    Location,
    Employer,
    Parent,
    Child,
    Related,
    Custom(String),
}

impl LinkType {
    /// Get string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Manager => "manager",
            Self::Member => "member",
            Self::Owner => "owner",
            Self::Author => "author",
            Self::Email => "email",
            Self::Deadline => "deadline",
            Self::Location => "location",
            Self::Employer => "employer",
            Self::Parent => "parent",
            Self::Child => "child",
            Self::Related => "related",
            Self::Custom(s) => s.as_str(),
        }
    }

    /// Parse link type from string.
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "manager" | "manages" | "managed_by" => Self::Manager,
            "member" | "member_of" => Self::Member,
            "owner" | "owns" | "owned_by" => Self::Owner,
            "author" | "wrote" | "written_by" => Self::Author,
            "email" | "contact" => Self::Email,
            "deadline" | "due" | "due_date" => Self::Deadline,
            "location" | "located_in" | "at" => Self::Location,
            "employer" | "works_at" | "employed_by" => Self::Employer,
            "parent" | "parent_of" => Self::Parent,
            "child" | "child_of" => Self::Child,
            "related" | "related_to" => Self::Related,
            other => Self::Custom(other.to_string()),
        }
    }
}

/// Edge direction for adjacency traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
}

/// Result from `follow()` traversal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowResult {
    /// Entity name found.
    pub node: String,
    /// Entity kind.
    pub kind: EntityKind,
    /// Confidence score (0.0-1.0).
    pub confidence: f32,
    /// Frame IDs where this entity appears.
    pub frame_ids: Vec<FrameId>,
    /// Path length from start node.
    pub path_length: usize,
}

/// Statistics about the logic mesh.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogicMeshStats {
    /// Total node count.
    pub node_count: usize,
    /// Total edge count.
    pub edge_count: usize,
    /// Count by entity kind.
    pub entity_kinds: HashMap<String, usize>,
    /// Count by link type.
    pub link_types: HashMap<String, usize>,
}

/// Complete Logic-Mesh graph structure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogicMesh {
    /// All nodes, sorted by id for determinism.
    pub nodes: Vec<MeshNode>,
    /// All edges, sorted by (`from_node`, `to_node`, link) for determinism.
    pub edges: Vec<MeshEdge>,
    /// Adjacency list: `node_id` → [(`edge_idx`, direction)].
    /// Built on load, not serialized.
    #[serde(skip)]
    adjacency: HashMap<u64, Vec<(usize, EdgeDirection)>>,
}

impl LogicMesh {
    /// Create an empty logic mesh.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the mesh is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    /// Get statistics about the mesh.
    #[must_use]
    pub fn stats(&self) -> LogicMeshStats {
        let mut entity_kinds = HashMap::new();
        for node in &self.nodes {
            *entity_kinds
                .entry(node.kind.as_str().to_string())
                .or_insert(0) += 1;
        }

        let mut link_types = HashMap::new();
        for edge in &self.edges {
            *link_types
                .entry(edge.link.as_str().to_string())
                .or_insert(0) += 1;
        }

        LogicMeshStats {
            node_count: self.nodes.len(),
            edge_count: self.edges.len(),
            entity_kinds,
            link_types,
        }
    }

    /// Serialize mesh to bytes with magic header.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(LOGIC_MESH_MAGIC);
        buf.extend_from_slice(&LOGIC_MESH_VERSION.to_le_bytes());

        // Sort for determinism before serialization
        let mut sorted = self.clone();
        sorted.nodes.sort_by_key(|n| n.id);
        sorted.edges.sort_by(|a, b| {
            (a.from_node, a.to_node, a.link.as_str()).cmp(&(
                b.from_node,
                b.to_node,
                b.link.as_str(),
            ))
        });

        // Serialize with bincode
        let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .with_little_endian();
        let payload = bincode::serde::encode_to_vec(&sorted, config).map_err(|e| {
            MemvidError::InvalidLogicMesh {
                reason: format!("serialization failed: {e}").into(),
            }
        })?;

        // Compress with zstd
        let compressed =
            zstd::encode_all(payload.as_slice(), 3).map_err(|e| MemvidError::InvalidLogicMesh {
                reason: format!("compression failed: {e}").into(),
            })?;

        buf.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        buf.extend(compressed);

        Ok(buf)
    }

    /// Deserialize mesh from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 14 {
            return Err(MemvidError::InvalidLogicMesh {
                reason: "blob too short".into(),
            });
        }

        if &bytes[0..4] != LOGIC_MESH_MAGIC {
            return Err(MemvidError::InvalidLogicMesh {
                reason: "invalid magic bytes".into(),
            });
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version > LOGIC_MESH_VERSION {
            return Err(MemvidError::InvalidLogicMesh {
                reason: format!("unsupported version: {version}").into(),
            });
        }

        let compressed_len = usize::try_from(u64::from_le_bytes(bytes[6..14].try_into().map_err(
            |_| MemvidError::InvalidLogicMesh {
                reason: "invalid length header".into(),
            },
        )?))
        .unwrap_or(0);

        if bytes.len() < 14 + compressed_len {
            return Err(MemvidError::InvalidLogicMesh {
                reason: "truncated blob".into(),
            });
        }

        let compressed = &bytes[14..14 + compressed_len];
        let decompressed =
            zstd::decode_all(compressed).map_err(|e| MemvidError::InvalidLogicMesh {
                reason: format!("decompression failed: {e}").into(),
            })?;

        let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .with_little_endian();
        let (mut mesh, _): (LogicMesh, _) =
            bincode::serde::decode_from_slice(&decompressed, config).map_err(|e| {
                MemvidError::InvalidLogicMesh {
                    reason: format!("deserialization failed: {e}").into(),
                }
            })?;

        // Validate bounds
        if mesh.nodes.len() > MAX_MESH_NODES {
            return Err(MemvidError::InvalidLogicMesh {
                reason: format!("too many nodes: {}", mesh.nodes.len()).into(),
            });
        }
        if mesh.edges.len() > MAX_MESH_EDGES {
            return Err(MemvidError::InvalidLogicMesh {
                reason: format!("too many edges: {}", mesh.edges.len()).into(),
            });
        }

        mesh.build_adjacency();
        Ok(mesh)
    }

    /// Build adjacency index from edges.
    pub fn build_adjacency(&mut self) {
        self.adjacency.clear();
        for (idx, edge) in self.edges.iter().enumerate() {
            self.adjacency
                .entry(edge.from_node)
                .or_default()
                .push((idx, EdgeDirection::Outgoing));
            self.adjacency
                .entry(edge.to_node)
                .or_default()
                .push((idx, EdgeDirection::Incoming));
        }
    }

    /// Find node by canonical name (case-insensitive).
    #[must_use]
    pub fn find_node(&self, name: &str) -> Option<&MeshNode> {
        let canonical = name.to_lowercase();
        let canonical = canonical.trim();
        self.nodes
            .iter()
            .find(|n| n.canonical_name == canonical || n.display_name.to_lowercase() == canonical)
    }

    /// Find node by ID.
    #[must_use]
    pub fn find_node_by_id(&self, id: u64) -> Option<&MeshNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Follow edges from a start node.
    #[must_use]
    pub fn follow(&self, start: &str, link: &str, hops: usize) -> Vec<FollowResult> {
        let Some(start_node) = self.find_node(start) else {
            return Vec::new();
        };

        let link_type = LinkType::from_str(link);
        let mut results = Vec::new();
        let mut visited = HashSet::new();
        let mut frontier = vec![(start_node.id, 0usize)];

        while let Some((node_id, depth)) = frontier.pop() {
            if depth >= hops || visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id);

            if let Some(adj) = self.adjacency.get(&node_id) {
                for &(edge_idx, direction) in adj {
                    let edge = &self.edges[edge_idx];

                    // Match link type
                    if edge.link.as_str() != link_type.as_str() {
                        continue;
                    }

                    let target_id = match direction {
                        EdgeDirection::Outgoing => edge.to_node,
                        EdgeDirection::Incoming => edge.from_node,
                    };

                    if let Some(target_node) = self.find_node_by_id(target_id) {
                        results.push(FollowResult {
                            node: target_node.display_name.clone(),
                            kind: target_node.kind,
                            confidence: edge.confidence_f32(),
                            frame_ids: target_node.frame_ids.clone(),
                            path_length: depth + 1,
                        });

                        if depth + 1 < hops {
                            frontier.push((target_id, depth + 1));
                        }
                    }
                }
            }
        }

        // Sort by confidence descending
        results.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results
    }

    /// Merge a node into the mesh, deduplicating by canonical name + kind.
    pub fn merge_node(&mut self, node: MeshNode) {
        // Find existing node by canonical name and kind
        if let Some(existing) = self
            .nodes
            .iter_mut()
            .find(|n| n.canonical_name == node.canonical_name && n.kind == node.kind)
        {
            // Merge frame_ids
            for fid in node.frame_ids {
                if !existing.frame_ids.contains(&fid) {
                    existing.frame_ids.push(fid);
                }
            }
            // Merge mentions
            existing.mentions.extend(node.mentions);
            // Update confidence to max
            existing.confidence = existing.confidence.max(node.confidence);
        } else {
            self.nodes.push(node);
        }
    }

    /// Merge an edge into the mesh, deduplicating by (from, to, link).
    pub fn merge_edge(&mut self, edge: MeshEdge) {
        // Deduplicate edges
        if !self.edges.iter().any(|e| {
            e.from_node == edge.from_node
                && e.to_node == edge.to_node
                && e.link.as_str() == edge.link.as_str()
        }) {
            self.edges.push(edge);
        }
    }

    /// Prepare the mesh for serialization (sort and rebuild adjacency).
    pub fn finalize(&mut self) {
        self.nodes.sort_by_key(|n| n.id);
        self.edges.sort_by(|a, b| {
            (a.from_node, a.to_node, a.link.as_str()).cmp(&(
                b.from_node,
                b.to_node,
                b.link.as_str(),
            ))
        });
        self.build_adjacency();
    }
}

/// Compute deterministic node ID from canonical name and kind.
#[must_use]
pub fn compute_node_id(canonical_name: &str, kind: EntityKind) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical_name.hash(&mut hasher);
    (kind as u8).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mesh_roundtrip() {
        let mut mesh = LogicMesh::new();

        mesh.merge_node(MeshNode::new(
            "sarah lee".to_string(),
            "Sarah Lee".to_string(),
            EntityKind::Person,
            0.95,
            0,
            0,
            10,
        ));

        mesh.merge_node(MeshNode::new(
            "project alpha".to_string(),
            "Project Alpha".to_string(),
            EntityKind::Project,
            0.88,
            0,
            20,
            13,
        ));

        let sarah_id = compute_node_id("sarah lee", EntityKind::Person);
        let project_id = compute_node_id("project alpha", EntityKind::Project);

        mesh.merge_edge(MeshEdge::new(
            sarah_id,
            project_id,
            LinkType::Manager,
            0.90,
            0,
        ));

        mesh.finalize();

        let bytes = mesh.serialize().expect("serialize");
        let restored = LogicMesh::deserialize(&bytes).expect("deserialize");

        assert_eq!(mesh.nodes.len(), restored.nodes.len());
        assert_eq!(mesh.edges.len(), restored.edges.len());
    }

    #[test]
    fn test_follow() {
        let mut mesh = LogicMesh::new();

        mesh.merge_node(MeshNode::new(
            "sarah lee".to_string(),
            "Sarah Lee".to_string(),
            EntityKind::Person,
            0.95,
            0,
            0,
            10,
        ));

        mesh.merge_node(MeshNode::new(
            "project alpha".to_string(),
            "Project Alpha".to_string(),
            EntityKind::Project,
            0.88,
            0,
            20,
            13,
        ));

        let sarah_id = compute_node_id("sarah lee", EntityKind::Person);
        let project_id = compute_node_id("project alpha", EntityKind::Project);

        mesh.merge_edge(MeshEdge::new(
            project_id,
            sarah_id,
            LinkType::Manager,
            0.90,
            0,
        ));

        mesh.finalize();

        let results = mesh.follow("Project Alpha", "manager", 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node, "Sarah Lee");
    }

    #[test]
    fn test_deterministic_serialization() {
        let mut mesh1 = LogicMesh::new();
        let mut mesh2 = LogicMesh::new();

        // Add in different orders
        mesh1.merge_node(MeshNode::new(
            "alice".to_string(),
            "Alice".to_string(),
            EntityKind::Person,
            0.9,
            0,
            0,
            5,
        ));
        mesh1.merge_node(MeshNode::new(
            "bob".to_string(),
            "Bob".to_string(),
            EntityKind::Person,
            0.8,
            1,
            0,
            3,
        ));

        mesh2.merge_node(MeshNode::new(
            "bob".to_string(),
            "Bob".to_string(),
            EntityKind::Person,
            0.8,
            1,
            0,
            3,
        ));
        mesh2.merge_node(MeshNode::new(
            "alice".to_string(),
            "Alice".to_string(),
            EntityKind::Person,
            0.9,
            0,
            0,
            5,
        ));

        mesh1.finalize();
        mesh2.finalize();

        let bytes1 = mesh1.serialize().expect("serialize 1");
        let bytes2 = mesh2.serialize().expect("serialize 2");

        assert_eq!(bytes1, bytes2, "Serialization must be deterministic");
    }
}
