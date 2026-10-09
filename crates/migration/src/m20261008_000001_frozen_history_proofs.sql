ALTER TABLE compaction_operation ADD COLUMN frozen_publication_contract TEXT NOT NULL DEFAULT 'legacy_unknown' CHECK(frozen_publication_contract IN ('legacy_unknown','assertion_compat','native_frozen'));

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_accounting_mode TEXT NOT NULL DEFAULT 'legacy_bound' CHECK(frozen_accounting_mode IN ('legacy_bound','live_known','completed_inventory'));

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_inventory_state TEXT NOT NULL DEFAULT 'unknown' CHECK(frozen_inventory_state IN ('unknown','pending','known'));

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_checkpoint_count INTEGER;

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_prepared_checkpoint_count INTEGER;

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_backfill_after_checkpoint TEXT;

-- statement

ALTER TABLE compaction_operation ADD COLUMN frozen_proof_state TEXT NOT NULL DEFAULT 'legacy' CHECK(frozen_proof_state IN ('legacy','pending','prepared','complete','quarantined')) CHECK((frozen_accounting_mode='legacy_bound' AND frozen_inventory_state='unknown' AND frozen_checkpoint_count IS NULL AND frozen_prepared_checkpoint_count IS NULL) OR (frozen_accounting_mode IN ('live_known','completed_inventory') AND frozen_checkpoint_count IS NOT NULL AND frozen_prepared_checkpoint_count IS NOT NULL AND frozen_prepared_checkpoint_count>=0 AND frozen_prepared_checkpoint_count<=frozen_checkpoint_count AND ((frozen_accounting_mode='live_known' AND frozen_inventory_state='known') OR (frozen_accounting_mode='completed_inventory' AND frozen_inventory_state IN ('pending','known'))))) CHECK(frozen_accounting_mode<>'live_known' OR frozen_publication_contract='native_frozen') CHECK(frozen_proof_state<>'complete' OR (frozen_inventory_state='known' AND frozen_checkpoint_count IS NOT NULL AND frozen_prepared_checkpoint_count IS NOT NULL AND frozen_checkpoint_count=frozen_prepared_checkpoint_count AND frozen_publication_contract<>'assertion_compat')) CHECK(frozen_publication_contract<>'assertion_compat' OR (frozen_accounting_mode='legacy_bound' AND frozen_inventory_state='unknown' AND frozen_checkpoint_count IS NULL AND frozen_prepared_checkpoint_count IS NULL AND frozen_backfill_after_checkpoint IS NULL AND frozen_proof_state='legacy'));

-- statement

ALTER TABLE compaction_checkpoint ADD COLUMN frozen_accounting_state TEXT NOT NULL DEFAULT 'uncounted' CHECK(frozen_accounting_state IN ('uncounted','counted','prepared'));

-- statement

ALTER TABLE compaction_operation_projection ADD COLUMN storage_state TEXT NOT NULL DEFAULT 'bound' CHECK(storage_state IN ('bound','proof_only'));

-- statement

ALTER TABLE compaction_frozen_history ADD COLUMN availability TEXT NOT NULL DEFAULT 'resident' CHECK(availability IN ('resident','releasing','released','quarantined')) CHECK(availability NOT IN ('releasing','released') OR ready=0);

-- statement

ALTER TABLE compaction_frozen_history ADD COLUMN storage_generation INTEGER NOT NULL DEFAULT 0 CHECK(storage_generation>=0);

-- statement

ALTER TABLE compaction_frozen_history ADD COLUMN release_kind INTEGER NOT NULL DEFAULT 0 CHECK(release_kind IN (0,1,2));

-- statement

ALTER TABLE compaction_frozen_history ADD COLUMN release_after_span INTEGER CHECK(release_after_span IS NULL OR release_after_span>=0);

-- statement

ALTER TABLE task_run_conversation_snapshot ADD COLUMN frozen_manifest_id TEXT;

-- statement

ALTER TABLE task_run_conversation_snapshot ADD COLUMN frozen_root_state TEXT NOT NULL DEFAULT 'unknown' CHECK(frozen_root_state IN ('unknown','none','manifest','blocked')) CHECK((frozen_root_state='manifest')=(frozen_manifest_id IS NOT NULL));

-- statement

CREATE INDEX task_run_conversation_snapshot_frozen_root ON task_run_conversation_snapshot(frozen_manifest_id,run_id);

-- statement

CREATE INDEX task_run_conversation_snapshot_frozen_barrier ON task_run_conversation_snapshot(workspace_id,frozen_root_state,run_id);

-- statement

ALTER TABLE turn_runtime_snapshot ADD COLUMN frozen_manifest_id TEXT;

-- statement

ALTER TABLE turn_runtime_snapshot ADD COLUMN frozen_root_state TEXT NOT NULL DEFAULT 'unknown' CHECK(frozen_root_state IN ('unknown','none','manifest','blocked')) CHECK((frozen_root_state='manifest')=(frozen_manifest_id IS NOT NULL));

-- statement

CREATE INDEX turn_runtime_snapshot_frozen_root ON turn_runtime_snapshot(frozen_manifest_id,turn_id);

-- statement

CREATE INDEX turn_runtime_snapshot_frozen_barrier ON turn_runtime_snapshot(workspace_id,frozen_root_state,turn_id);

-- statement

ALTER TABLE thread_cli_runtime_binding ADD COLUMN frozen_manifest_id TEXT;

-- statement

ALTER TABLE thread_cli_runtime_binding ADD COLUMN frozen_root_state TEXT NOT NULL DEFAULT 'unknown' CHECK(frozen_root_state IN ('unknown','none','manifest','blocked')) CHECK((frozen_root_state='manifest')=(frozen_manifest_id IS NOT NULL));

-- statement

CREATE INDEX thread_cli_runtime_binding_frozen_root ON thread_cli_runtime_binding(frozen_manifest_id,thread_id);

-- statement

CREATE INDEX thread_cli_runtime_binding_frozen_barrier ON thread_cli_runtime_binding(workspace_id,frozen_root_state,thread_id);

-- statement

ALTER TABLE turn_cli_runtime_binding ADD COLUMN frozen_manifest_id TEXT;

-- statement

ALTER TABLE turn_cli_runtime_binding ADD COLUMN frozen_root_state TEXT NOT NULL DEFAULT 'unknown' CHECK(frozen_root_state IN ('unknown','none','manifest','blocked')) CHECK((frozen_root_state='manifest')=(frozen_manifest_id IS NOT NULL));

-- statement

CREATE INDEX turn_cli_runtime_binding_frozen_root ON turn_cli_runtime_binding(frozen_manifest_id,turn_id);

-- statement

CREATE INDEX turn_cli_runtime_binding_frozen_barrier ON turn_cli_runtime_binding(workspace_id,frozen_root_state,turn_id);

-- statement

CREATE TABLE compaction_checkpoint_proof (
 checkpoint_id TEXT PRIMARY KEY NOT NULL REFERENCES compaction_checkpoint(id) ON DELETE CASCADE,
 operation_id TEXT NOT NULL, owner TEXT NOT NULL, workspace_id TEXT NOT NULL, thread_id TEXT NOT NULL,
 checkpoint_identity_sha256 TEXT NOT NULL, previous TEXT,
 projection_version INTEGER NOT NULL CHECK(projection_version>=0), format_version INTEGER NOT NULL CHECK(format_version>=0),
 coverage_domain TEXT NOT NULL CHECK(coverage_domain IN ('own_contribution','working_context')),
 origin_manifest_id TEXT NOT NULL, origin_message_count INTEGER NOT NULL CHECK(origin_message_count>=0),
 origin_identity_sha256 TEXT NOT NULL, origin_import_count INTEGER NOT NULL CHECK(origin_import_count>=0), origin_imports_sha256 TEXT NOT NULL,
 coverage_count INTEGER NOT NULL CHECK(coverage_count>=0), coverage_sha256 TEXT NOT NULL,
 alias_count INTEGER NOT NULL CHECK(alias_count>=0), aliases_sha256 TEXT NOT NULL, next_alias INTEGER NOT NULL DEFAULT 0 CHECK(next_alias>=0 AND next_alias<=alias_count),
 evidence_count INTEGER NOT NULL CHECK(evidence_count>=0), evidence_sha256 TEXT NOT NULL, next_evidence INTEGER NOT NULL DEFAULT 0 CHECK(next_evidence>=0 AND next_evidence<=evidence_count),
 proof_format INTEGER NOT NULL DEFAULT 1 CHECK(proof_format=1), state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','prepared','quarantined')),
 CHECK(state<>'prepared' OR (next_alias=alias_count AND next_evidence=evidence_count))
);

-- statement

CREATE TABLE compaction_checkpoint_replay_proof (
 checkpoint_id TEXT NOT NULL REFERENCES compaction_checkpoint_proof(checkpoint_id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 covered_thread TEXT NOT NULL, covered_scope TEXT NOT NULL, covered_id TEXT NOT NULL, covered_version TEXT NOT NULL,
 replay_thread TEXT NOT NULL, replay_scope TEXT NOT NULL, replay_id TEXT NOT NULL, replay_version TEXT NOT NULL,
 tool_item_id TEXT, PRIMARY KEY(checkpoint_id,ordinal)
);

-- statement

CREATE TABLE compaction_checkpoint_event_input_proof (
 checkpoint_id TEXT NOT NULL REFERENCES compaction_checkpoint_proof(checkpoint_id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0), source_thread TEXT NOT NULL, source_scope TEXT NOT NULL,
 source_id TEXT NOT NULL, source_version TEXT NOT NULL,
 role TEXT NOT NULL CHECK(role IN ('authoritative','deleted','input_copy')), PRIMARY KEY(checkpoint_id,ordinal)
);

-- statement

CREATE TABLE compaction_frozen_use (
 use_id TEXT NOT NULL, manifest_id TEXT NOT NULL REFERENCES compaction_frozen_history(id) ON DELETE CASCADE,
 storage_generation INTEGER NOT NULL CHECK(storage_generation>=0), purpose TEXT NOT NULL CHECK(purpose IN ('read','capture','layout','proof')),
 workspace_id TEXT NOT NULL, PRIMARY KEY(use_id,manifest_id)
);

-- statement

CREATE TABLE compaction_frozen_maintenance_progress (
 workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
 phase TEXT NOT NULL CHECK(phase IN ('root_task','root_runtime','root_cli_thread','root_cli_turn','proof_backfill','reclaim','cleanup_seed','detach','logical_reclaim')),
 after_key TEXT, state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','complete','blocked')),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1), PRIMARY KEY(workspace_id,phase)
);

-- statement

CREATE TABLE compaction_frozen_cleanup (
 manifest_id TEXT NOT NULL REFERENCES compaction_frozen_history(id) ON DELETE CASCADE, kind INTEGER NOT NULL CHECK(kind IN (0,1)),
 state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('idle','queued','running','quarantined')),
 storage_generation INTEGER NOT NULL CHECK(storage_generation>=0), dirty_seq INTEGER NOT NULL DEFAULT 0 CHECK(dirty_seq>=0),
 pass_no INTEGER NOT NULL DEFAULT 0 CHECK(pass_no>=0), pass_seq INTEGER CHECK(pass_seq IS NULL OR pass_seq>=0),
 after_ordinal INTEGER CHECK(after_ordinal IS NULL OR after_ordinal>=0), ceiling_ordinal INTEGER CHECK(ceiling_ordinal IS NULL OR ceiling_ordinal>=-1),
 PRIMARY KEY(manifest_id,kind), CHECK(state<>'running' OR (pass_seq IS NOT NULL AND ceiling_ordinal IS NOT NULL AND (after_ordinal IS NULL OR after_ordinal<=ceiling_ordinal)))
);

-- statement

CREATE INDEX checkpoint_proof_operation ON compaction_checkpoint_proof(operation_id,state,checkpoint_id);

-- statement

CREATE INDEX checkpoint_operation_inventory ON compaction_checkpoint(operation_id,id);

-- statement

CREATE INDEX checkpoint_operation_accounting ON compaction_checkpoint(operation_id,frozen_accounting_state,id);

-- statement

CREATE INDEX operation_frozen_proof ON compaction_operation(frozen_proof_state,status,id);

-- statement

CREATE INDEX projection_frozen_root ON compaction_operation_projection(manifest_id,storage_state,operation_id);

-- statement

CREATE INDEX frozen_use_manifest ON compaction_frozen_use(manifest_id,storage_generation,use_id);

-- statement

CREATE INDEX frozen_availability ON compaction_frozen_history(workspace_id,availability,id);

-- statement

CREATE INDEX frozen_output_manifest ON compaction_task_output(manifest_id,task_run_turn_id);

-- statement

CREATE INDEX frozen_inactive_candidate ON compaction_frozen_layout(candidate,manifest_id,kind) WHERE active=0 AND candidate IS NOT NULL;

-- statement

CREATE INDEX frozen_cleanup_runnable ON compaction_frozen_cleanup(manifest_id,kind) WHERE state IN ('queued','running');

-- statement

DROP INDEX frozen_history_content;

-- statement

CREATE INDEX frozen_history_content ON compaction_frozen_history(workspace_id,owner_thread,ready,availability,identity_sha256,imports_sha256);

-- statement

DROP TRIGGER compaction_task_basis_update;

-- statement

CREATE TRIGGER compaction_task_basis_update AFTER UPDATE OF run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at ON task_run_conversation_snapshot BEGIN INSERT INTO compaction_task_basis_revision(run_id,revision) VALUES (OLD.run_id,2) ON CONFLICT(run_id) DO UPDATE SET revision=revision+1; INSERT INTO compaction_projection_epoch(thread_id,version) SELECT id,1 FROM thread WHERE id IN (OLD.conversation_thread_id,NEW.conversation_thread_id) ON CONFLICT(thread_id) DO UPDATE SET version=version+1; END;

-- statement

CREATE TRIGGER frozen_dirty_span_delete AFTER DELETE ON compaction_frozen_span BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,OLD.kind,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.source_manifest ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_span_update AFTER UPDATE ON compaction_frozen_span WHEN OLD.source_manifest IS NOT NEW.source_manifest OR OLD.kind<>NEW.kind OR OLD.start<>NEW.start OR NEW.end<OLD.end BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,OLD.kind,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.source_manifest ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_layout_delete AFTER DELETE ON compaction_frozen_layout BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,OLD.kind,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.candidate AND OLD.active=0 AND OLD.candidate IS NOT NULL ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.candidate AND OLD.active=0 AND OLD.candidate IS NOT NULL ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_layout_update AFTER UPDATE OF active,candidate ON compaction_frozen_layout WHEN OLD.active<>NEW.active OR OLD.candidate IS NOT NEW.candidate BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,OLD.kind,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.candidate AND OLD.active=0 AND OLD.candidate IS NOT NULL AND (NEW.active<>0 OR OLD.candidate IS NOT NEW.candidate) ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.candidate AND OLD.active=0 AND OLD.candidate IS NOT NULL AND (NEW.active<>0 OR OLD.candidate IS NOT NEW.candidate) ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_use_delete AFTER DELETE ON compaction_frozen_use BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=OLD.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_header_insert AFTER INSERT ON compaction_frozen_history BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_header_update AFTER UPDATE OF availability,storage_generation ON compaction_frozen_history WHEN OLD.availability<>NEW.availability OR OLD.storage_generation<>NEW.storage_generation BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_message_insert AFTER INSERT ON compaction_frozen_message_data BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,0,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

CREATE TRIGGER frozen_dirty_import_insert AFTER INSERT ON compaction_frozen_import_data BEGIN
INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation,dirty_seq) SELECT id,1,storage_generation,1 FROM compaction_frozen_history WHERE id=NEW.manifest_id ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=compaction_frozen_cleanup.dirty_seq+1, state=CASE WHEN compaction_frozen_cleanup.state='quarantined' THEN 'quarantined' WHEN compaction_frozen_cleanup.storage_generation<>excluded.storage_generation OR compaction_frozen_cleanup.state='idle' THEN 'queued' ELSE compaction_frozen_cleanup.state END, storage_generation=excluded.storage_generation;
END;

-- statement

DROP VIEW compaction_frozen_message;

-- statement

CREATE VIEW compaction_frozen_message AS
 SELECT d.manifest_id,d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_message_data d
 JOIN compaction_frozen_history h ON h.id=d.manifest_id
 WHERE h.availability='resident' AND d.ordinal>=0 AND d.ordinal<h.message_count
 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=d.manifest_id AND l.kind=0 AND l.active=1)
 UNION ALL
 SELECT s.manifest_id,d.ordinal,d.reference_json,d.bytes FROM compaction_frozen_span s
 JOIN compaction_frozen_message_data d ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end
 JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1
 JOIN compaction_frozen_history h ON h.id=s.manifest_id
 WHERE s.kind=0 AND h.availability='resident' AND d.ordinal>=0 AND d.ordinal<h.message_count;

-- statement

DROP VIEW compaction_frozen_import;

-- statement

CREATE VIEW compaction_frozen_import AS
 SELECT d.manifest_id,d.ordinal,d.message_ordinal,d.source_scope,d.source_id,d.source_version,d.source_thread,d.proof_json,d.bytes FROM compaction_frozen_import_data d
 JOIN compaction_frozen_history h ON h.id=d.manifest_id
 WHERE h.availability='resident' AND d.ordinal>=0 AND d.ordinal<h.import_count
 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout l WHERE l.manifest_id=d.manifest_id AND l.kind=1 AND l.active=1)
 UNION ALL
 SELECT s.manifest_id,d.ordinal,d.message_ordinal,d.source_scope,d.source_id,d.source_version,d.source_thread,d.proof_json,d.bytes FROM compaction_frozen_span s
 JOIN compaction_frozen_import_data d ON d.manifest_id=s.source_manifest AND d.ordinal>=s.start AND d.ordinal<s.end
 JOIN compaction_frozen_layout l ON l.manifest_id=s.manifest_id AND l.kind=s.kind AND l.active=1
 JOIN compaction_frozen_history h ON h.id=s.manifest_id
 WHERE s.kind=1 AND h.availability='resident' AND d.ordinal>=0 AND d.ordinal<h.import_count;

-- statement
CREATE INDEX task_run_conversation_snapshot_frozen_inventory ON task_run_conversation_snapshot(workspace_id,run_id);

-- statement
CREATE INDEX turn_runtime_snapshot_frozen_inventory ON turn_runtime_snapshot(workspace_id,turn_id);

-- statement
CREATE INDEX thread_cli_runtime_binding_frozen_inventory ON thread_cli_runtime_binding(workspace_id,thread_id);

-- statement
CREATE INDEX turn_cli_runtime_binding_frozen_inventory ON turn_cli_runtime_binding(workspace_id,turn_id);

-- statement
CREATE INDEX compaction_frozen_history_frozen_inventory ON compaction_frozen_history(workspace_id,id);
