-- A thread cascade removes both logical manifests and their physical backing.
-- Other live spans must still prevent deletion of backing at transaction commit.
ALTER TABLE "compaction_frozen_span"
    ALTER CONSTRAINT "compaction_frozen_span_source_manifest_fkey"
    DEFERRABLE INITIALLY DEFERRED;
