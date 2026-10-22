//! Logic-Mesh extensions for `Memvid`.
//!
//! This module provides methods for managing the Logic-Mesh entity-relationship
//! graph within an MV2 file, including adding nodes/edges, traversing relationships,
//! and querying entities.

use crate::memvid::lifecycle::Memvid;
use crate::types::{
    EntityKind, FollowResult, FrameId, LogicMesh, LogicMeshStats, MeshEdge, MeshNode,
    SearchHitEntity,
};

impl Memvid {
    /// Get an immutable reference to the Logic-Mesh.
    ///
    /// Returns the in-memory Logic-Mesh. Changes are persisted when
    /// the file is committed or sealed.
    #[must_use]
    pub fn logic_mesh(&self) -> &LogicMesh {
        &self.logic_mesh
    }

    /// Get a mutable reference to the Logic-Mesh.
    ///
    /// Returns the in-memory Logic-Mesh for direct manipulation.
    /// Changes are persisted when the file is committed or sealed.
    pub fn logic_mesh_mut(&mut self) -> &mut LogicMesh {
        self.dirty = true;
        &mut self.logic_mesh
    }

    /// Replace the entire Logic-Mesh with a new one.
    ///
    /// This is useful after building a mesh from NER extraction.
    /// Changes are persisted when the file is committed or sealed.
    pub fn set_logic_mesh(&mut self, mesh: LogicMesh) {
        self.dirty = true;
        self.logic_mesh = mesh;
    }

    /// Add a mesh node (entity) to the Logic-Mesh.
    ///
    /// The node is merged with existing nodes by canonical name and kind.
    /// If a matching node exists, mentions and frame IDs are combined.
    ///
    /// # Arguments
    /// * `node` - The mesh node to add
    pub fn add_mesh_node(&mut self, node: MeshNode) {
        self.dirty = true;
        self.logic_mesh.merge_node(node);
    }

    /// Add multiple mesh nodes at once.
    ///
    /// # Arguments
    /// * `nodes` - The mesh nodes to add
    pub fn add_mesh_nodes(&mut self, nodes: Vec<MeshNode>) {
        self.dirty = true;
        for node in nodes {
            self.logic_mesh.merge_node(node);
        }
    }

    /// Add a mesh edge (relationship) to the Logic-Mesh.
    ///
    /// The edge is deduplicated by (from, to, `link_type`).
    ///
    /// # Arguments
    /// * `edge` - The mesh edge to add
    pub fn add_mesh_edge(&mut self, edge: MeshEdge) {
        self.dirty = true;
        self.logic_mesh.merge_edge(edge);
    }

    /// Add multiple mesh edges at once.
    ///
    /// # Arguments
    /// * `edges` - The mesh edges to add
    pub fn add_mesh_edges(&mut self, edges: Vec<MeshEdge>) {
        self.dirty = true;
        for edge in edges {
            self.logic_mesh.merge_edge(edge);
        }
    }

    /// Follow relationships from an entity in the graph.
    ///
    /// Traverses the Logic-Mesh starting from the named entity,
    /// following edges of the specified type up to the given number of hops.
    ///
    /// # Arguments
    /// * `start` - The entity name to start from (case-insensitive)
    /// * `link` - The relationship type to follow (e.g., "manager", "employer")
    /// * `hops` - Maximum number of hops to traverse
    ///
    /// # Returns
    /// A list of entities found by traversing the relationships.
    #[must_use]
    pub fn follow(&self, start: &str, link: &str, hops: usize) -> Vec<FollowResult> {
        self.logic_mesh.follow(start, link, hops)
    }

    /// Find an entity node by name.
    ///
    /// # Arguments
    /// * `name` - The entity name to search for (case-insensitive)
    ///
    /// # Returns
    /// The matching node if found.
    #[must_use]
    pub fn find_entity(&self, name: &str) -> Option<&MeshNode> {
        self.logic_mesh.find_node(name)
    }

    /// Get all entities mentioned in a specific frame.
    ///
    /// # Arguments
    /// * `frame_id` - The frame ID to query
    ///
    /// # Returns
    /// A list of entity nodes that have mentions in the specified frame.
    #[must_use]
    pub fn frame_entities(&self, frame_id: FrameId) -> Vec<&MeshNode> {
        self.logic_mesh
            .nodes
            .iter()
            .filter(|node| node.frame_ids.contains(&frame_id))
            .collect()
    }

    /// Get all entities of a specific kind.
    ///
    /// # Arguments
    /// * `kind` - The entity kind to filter by
    ///
    /// # Returns
    /// A list of entity nodes matching the specified kind.
    #[must_use]
    pub fn entities_by_kind(&self, kind: EntityKind) -> Vec<&MeshNode> {
        self.logic_mesh
            .nodes
            .iter()
            .filter(|node| node.kind == kind)
            .collect()
    }

    /// Get statistics about the Logic-Mesh.
    ///
    /// # Returns
    /// Statistics including node count, edge count, and breakdowns by kind/link type.
    #[must_use]
    pub fn logic_mesh_stats(&self) -> LogicMeshStats {
        self.logic_mesh.stats()
    }

    /// Check if the Logic-Mesh has any content.
    ///
    /// # Returns
    /// `true` if the mesh has nodes or edges.
    #[must_use]
    pub fn has_logic_mesh(&self) -> bool {
        !self.logic_mesh.is_empty()
    }

    /// Get the number of entity nodes in the mesh.
    #[must_use]
    pub fn mesh_node_count(&self) -> usize {
        self.logic_mesh.nodes.len()
    }

    /// Get the number of relationship edges in the mesh.
    #[must_use]
    pub fn mesh_edge_count(&self) -> usize {
        self.logic_mesh.edges.len()
    }

    /// Get entities for a frame as `SearchHitEntity` for search metadata.
    ///
    /// Returns entities from the Logic-Mesh that appear in the given frame.
    #[must_use]
    pub fn frame_entities_for_search(&self, frame_id: FrameId) -> Vec<SearchHitEntity> {
        self.logic_mesh
            .nodes
            .iter()
            .filter(|node| node.frame_ids.contains(&frame_id))
            .map(|node| SearchHitEntity {
                name: node.display_name.clone(),
                kind: node.kind.as_str().to_string(),
                confidence: Some(node.confidence_f32()),
            })
            .collect()
    }
}
