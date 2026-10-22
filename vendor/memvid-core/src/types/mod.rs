//! Public types exposed by the `memvid-core` crate.

pub mod acl;
pub mod adaptive;
pub mod ask;
pub mod audit;
pub mod binding;
pub mod common;
pub mod embedding;
pub mod embedding_identity;
pub mod frame;
pub mod graph_query;
pub mod logic_mesh;
pub mod manifest;
pub mod memories_track;
pub mod memory_card;
pub mod metadata;
pub mod options;
pub mod reranker;
pub mod schema;
pub mod search;
pub mod sketch_track;
pub mod structure;
#[cfg(feature = "temporal_track")]
pub mod temporal;
pub mod ticket;
pub mod verification;

pub use ask::{
    AskCitation, AskContextFragment, AskContextFragmentKind, AskMode, AskRequest, AskResponse,
    AskRetriever, AskStats, VecEmbedder,
};
pub use audit::{AuditOptions, AuditReport, SourceSpan};
pub use binding::{FileInfo, MemoryBinding};
pub use common::{
    CanonicalEncoding, EnrichmentState, EnrichmentTask, FrameId, FrameRole, FrameStatus,
    MemvidHandle, Open, Sealed, Tier,
};
// AnchorSource always exported - not feature-gated to maintain binary compatibility
pub use frame::AnchorSource;
pub use frame::{Frame, Stats, TimelineEntry, TimelineQuery, TimelineQueryBuilder};
// Serialized manifest types - always exported for binary compatibility
pub use manifest::TemporalSegmentDescriptor;
pub use manifest::TemporalTrackManifest;
pub use manifest::{
    EnrichmentQueueManifest, Header, IndexManifests, IndexSegmentRef, LexIndexManifest,
    LexSegmentDescriptor, LexSegmentManifest, LogicMeshManifest, MemoriesTrackManifest,
    SegmentCatalog, SegmentCommon, SegmentCompression, SegmentKind, SegmentMeta, SegmentSpan,
    SegmentStats, SketchTrackManifest, TantivySegmentDescriptor, TimeIndexManifest,
    TimeSegmentDescriptor, Toc, VecIndexManifest, VecSegmentDescriptor, VectorCompression,
};
// Logic-Mesh types for entity-relationship graph traversal
pub use logic_mesh::{
    EdgeDirection, EntityKind, FollowResult, LOGIC_MESH_MAGIC, LOGIC_MESH_VERSION, LinkType,
    LogicMesh, LogicMeshStats, MeshEdge, MeshNode,
};
pub use metadata::{
    AudioSegmentMetadata, DocAudioMetadata, DocExifMetadata, DocGpsMetadata, DocMetadata,
    MediaManifest, TextChunkManifest, TextChunkRange,
};
pub use options::{PutManyOpts, PutOptions, PutOptionsBuilder, PutRequest};
pub use search::{
    SearchEngineKind, SearchHit, SearchHitEntity, SearchHitMetadata, SearchParams, SearchRequest,
    SearchResponse,
};
#[cfg(feature = "temporal_track")]
pub use search::{SearchHitTemporal, SearchHitTemporalAnchor, SearchHitTemporalMention};
#[cfg(feature = "temporal_track")]
pub use temporal::{
    TEMPORAL_TRACK_FLAG_HAS_ANCHORS, TEMPORAL_TRACK_FLAG_HAS_MENTIONS, TemporalAnchor,
    TemporalCapabilities, TemporalFilter, TemporalMention, TemporalMentionFlags,
    TemporalMentionKind, TemporalTrack,
};
pub use ticket::{SignedTicket, Ticket, TicketRef};
pub use verification::{
    DOCTOR_PLAN_VERSION, DoctorActionDetail, DoctorActionKind, DoctorActionPlan,
    DoctorActionReport, DoctorActionStatus, DoctorFinding, DoctorFindingCode, DoctorMetrics,
    DoctorOptions, DoctorPhaseDuration, DoctorPhaseKind, DoctorPhasePlan, DoctorPhaseReport,
    DoctorPhaseStatus, DoctorPlan, DoctorReport, DoctorSeverity, DoctorStatus, VerificationCheck,
    VerificationReport, VerificationStatus,
};
// Memory card types for structured memory extraction
pub use memories_track::{
    EngineStamp, EnrichmentManifest, EnrichmentRecord, MEMORIES_TRACK_MAGIC,
    MEMORIES_TRACK_VERSION, MemoriesStats, MemoriesTrack, SlotIndex,
};
pub use memory_card::{
    MemoryCard, MemoryCardBuilder, MemoryCardBuilderError, MemoryCardId, MemoryKind, Polarity,
    VersionRelation,
};
// Embedding provider types for vector embedding generation
pub use embedding::{
    BatchEmbeddingResult, EmbeddingConfig, EmbeddingProvider, EmbeddingProviderKind,
    EmbeddingResult,
};
pub use embedding_identity::{
    EmbeddingIdentity, EmbeddingIdentityCount, EmbeddingIdentitySummary,
    MEMVID_EMBEDDING_DIMENSION_KEY, MEMVID_EMBEDDING_MODEL_KEY, MEMVID_EMBEDDING_NORMALIZED_KEY,
    MEMVID_EMBEDDING_PROVIDER_KEY,
};
// Structure-aware chunking types for preserving tables and code blocks
pub use structure::{
    ChunkType, ChunkingOptions, ChunkingResult, CodeChunkingStrategy, DocumentElement, ElementData,
    ElementType, StructuredCell, StructuredChunk, StructuredCodeBlock, StructuredDocument,
    StructuredHeading, StructuredList, StructuredRow, StructuredTable, TableChunkingStrategy,
};
// Adaptive retrieval types for dynamic result set sizing
pub use acl::{
    ACL_POLICY_VERSION_KEY, ACL_READ_GROUPS_KEY, ACL_READ_PRINCIPALS_KEY, ACL_READ_ROLES_KEY,
    ACL_RESOURCE_ID_KEY, ACL_TENANT_ID_KEY, ACL_VISIBILITY_KEY, AclContext, AclEnforcementMode,
};
pub use adaptive::{
    AdaptiveConfig, AdaptiveResult, AdaptiveStats, CutoffStrategy, EmbeddingQualityStats,
    compute_embedding_quality, find_adaptive_cutoff, normalize_scores,
};
// Graph-aware query types for hybrid retrieval
pub use graph_query::{
    GraphMatchResult, GraphPattern, HybridSearchHit, PatternTerm, QueryPlan, TriplePattern,
};
// Schema types for predicate validation
pub use schema::{
    Cardinality, PredicateId, PredicateSchema, SchemaError, SchemaRegistry, ValueType,
};
// Sketch track types for fast candidate generation
pub use sketch_track::{
    DEFAULT_HAMMING_THRESHOLD, QuerySketch, SKETCH_TRACK_MAGIC, SKETCH_TRACK_VERSION, SketchEntry,
    SketchFlags, SketchTrack, SketchTrackHeader, SketchTrackStats, SketchVariant,
    build_term_filter, compute_simhash, compute_token_weights, generate_sketch, hash_token,
    hash_token_u32, read_sketch_track, term_filter_maybe_contains, tokenize_for_sketch,
    write_sketch_track,
};
