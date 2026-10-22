# Pioneer scoped retrieval patch

Vendored memvid-core 3.1.8, original commit fe0b3d9ac883c667587c262f7b372bf06eb605d5
from https://github.com/pioneerdotai/memory. Apache-2.0; see LICENSE.

Changes: expose committed generation; retain lexical continuation when the document
candidate window is full (zero-progress exhaustion is an explicit error); scoped
lexical pages retain BM25 order without window-relative recency reordering; progressively increase native vector candidate window
until URI scope has enough candidates or the backend window is exhausted.
Lexical URI scope is part of the native Tantivy query before TopDocs, using its
existing raw URI field; finite frame filters do not create false continuation.
HNSW query width grows with this request instead of remaining fixed at 50;
small scoped corpora stop after their active-frame count is satisfied. Inactive
frames cannot occupy the final vector top-k.
Native ask fusion and semantic reranking remain unchanged. HNSW remains approximate.
Only library source, required embedded data, manifest and license are included.
No dependencies were upgraded. Tests/builds have not been run.

PIONEER.patch records the manifest and source changes against that commit with
zero context (`git apply --unidiff-zero`); unchanged files retain upstream bytes.
