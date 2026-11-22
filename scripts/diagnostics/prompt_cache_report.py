#!/usr/bin/env python3
"""Read retained usage only. No Pioneer imports, model calls, or database writes.

Use the running Gateway runtime home and Python 3.14 or later for compressed events.
Use --turn for turn events, --owner for service-operation journal owners. Old
purpose attribution is explicit (--purpose OBSERVATION_ID=PURPOSE), never guessed
from sequence position. Codex session totals are separate, never turn charges.
"""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
from collections import defaultdict


def counter(value):
    return value if type(value) is int and 0 <= value <= 2**64 - 1 else None


def strict_sum(values):
    return counter(sum(values)) if all(v is not None for v in values) else None


def timestamp(value):
    if isinstance(value, (int, float)):
        return float(value)
    parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.timestamp()


MAX_EVENT_BYTES = 8 * 1024 * 1024
BLOCK_BYTES = 64 * 1024


def decode_frame(codec, body, size, chars, digest):
    data = bytes.fromhex(body)
    if codec == 1:
        # Import only for a compressed frame. The read has already released
        # the database connection and the frame cannot expand past its header.
        from compression.zstd import ZstdDecompressor

        if len(data) + 2 >= size:
            raise ValueError("invalid compressed event size")
        decoder = ZstdDecompressor()
        data = decoder.decompress(data, max_length=size + 1)
        if not decoder.eof or decoder.unused_data:
            raise ValueError("invalid or trailing event frame data")
    elif codec != 0:
        raise ValueError("unknown event encoding")
    if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("event frame checksum or length mismatch")
    text = data.decode("utf-8")
    if len(text) != chars:
        raise ValueError("event frame character length mismatch")
    return text


def decode_payload(inline, size, chars, digest, codec, body, parts):
    if inline is not None:
        if len(inline.encode("utf-8")) > MAX_EVENT_BYTES:
            raise ValueError("event exceeds the diagnostic byte limit")
        return inline
    if size is None or not 0 <= size <= MAX_EVENT_BYTES:
        raise ValueError("missing event payload or event exceeds the diagnostic byte limit")
    if codec is not None:
        if size > BLOCK_BYTES or parts:
            raise ValueError("invalid whole event payload")
        return decode_frame(codec, body, size, chars, digest)
    text = []
    consumed_bytes = consumed_chars = 0
    original_hash = hashlib.sha256()
    for offset, char_offset, block_codec, block_body, length, count, checksum in parts or ():
        if (offset != consumed_bytes or char_offset != consumed_chars or
                not 0 < length <= BLOCK_BYTES or consumed_bytes + length > size):
            raise ValueError("invalid event block range")
        part = decode_frame(block_codec, block_body, length, count, checksum)
        text.append(part)
        original_hash.update(part.encode("utf-8"))
        consumed_bytes += length
        consumed_chars += count
    if (size <= BLOCK_BYTES or consumed_bytes != size or consumed_chars != chars or
            original_hash.hexdigest() != digest):
        raise ValueError("event payload checksum or length mismatch")
    return "".join(text)


class GatewayPostgres:
    """Read one bounded result at a time via the PostgreSQL client shipped by Gateway."""

    def __init__(self, gateway, runtime_home):
        executable = pathlib.Path(gateway).resolve(strict=True)
        if executable.match("Pioneer Gateway.app/Contents/MacOS/pioneer"):
            runtime = executable.parents[3] / "postgres"
        else:
            runtime = executable.parent / "cua" / "postgres"
        self.psql = runtime / "bin" / ("psql.exe" if os.name == "nt" else "psql")
        postgres = pathlib.Path(runtime_home) / "postgres"
        pid = (postgres / "data" / "postmaster.pid").read_text().splitlines()
        port = int(pid[3])
        password = (postgres / "reader-password").read_text().strip()
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
        self.env.update(
            PGHOST="127.0.0.1", PGPORT=str(port), PGUSER="pioneer_reader", PGDATABASE="pioneer",
            PGPASSWORD=password, PGSSLMODE="disable", PGCLIENTENCODING="UTF8",
            PGOPTIONS="-c default_transaction_read_only=on",
        )
        if os.name == "posix":
            self.env["LD_LIBRARY_PATH"] = str(runtime / "lib")

    def read(self, sql, *parameters):
        command = [self.psql, "-XwqAt", "-v", "ON_ERROR_STOP=1"]
        for index, parameter in enumerate(parameters, start=1):
            command.extend(("-v", f"arg{index}={parameter}"))
        process = subprocess.run(
            command, input=sql + ";\n", text=True, encoding="utf-8", env=self.env,
            capture_output=True, timeout=30,
        )
        if process.returncode:
            raise RuntimeError("could not read PostgreSQL usage evidence from the configured Gateway")
        return [json.loads(line) for line in process.stdout.splitlines()]

    def runtime_snapshot(self, turn):
        rows = self.read("""
            SELECT json_build_array(workspace_id, thread_id, hook_runtime_context_json::text)
            FROM turn_runtime_snapshot WHERE turn_id = :'arg1'
        """, turn)
        return rows[0] if rows else None

    def task_bindings(self, turn):
        return self.read("""
            SELECT json_build_array(
                b.task_id, b.thread_id, t.workspace_id, t.owner_kind, t.owner_id,
                t.created_by_thread_id, t.created_by_turn_id, t.executor_kind,
                t.lifecycle_policy_json::text, t.metadata_json::text, e.workspace_id, x.thread_id,
                p.workspace_id, o.thread_id, r.task_id)
            FROM task_run_turn b LEFT JOIN task t ON t.id = b.task_id
            LEFT JOIN task_run r ON r.id = b.run_id
            LEFT JOIN thread e ON e.id = b.thread_id LEFT JOIN turn x ON x.id = b.turn_id
            LEFT JOIN thread p ON p.id = t.owner_id LEFT JOIN turn o ON o.id = t.created_by_turn_id
            WHERE b.turn_id = :'arg1' LIMIT 2
        """, turn)

    def event_page(self, turn, after):
        rows = self.read("""
            WITH events AS (
                SELECT id, thread_id, sequence, created_at, payload, payload_payload_id
                FROM turn_event
                WHERE turn_id = :'arg1' AND sequence > :'arg2'::bigint
                ORDER BY sequence LIMIT 16
            )
            SELECT json_build_array(
                e.id, e.thread_id, e.sequence, e.created_at,
                CASE WHEN octet_length(e.payload::text) <= 8388608 THEN e.payload::text END,
                v.original_bytes, v.original_chars, encode(v.sha256, 'hex'), v.codec,
                CASE WHEN v.original_bytes <= 8388608 THEN encode(v.body, 'hex') END, p.parts)
            FROM events e
            LEFT JOIN portable_payload_value v ON v.id = e.payload_payload_id
            LEFT JOIN LATERAL (
                SELECT json_agg(json_build_array(
                    byte_offset, char_offset, codec, encode(body, 'hex'), original_bytes,
                    original_chars, encode(sha256, 'hex')) ORDER BY byte_offset) AS parts
                FROM portable_payload_part
                WHERE payload_id = v.id AND v.codec IS NULL AND v.original_bytes <= 8388608
            ) p ON true
            ORDER BY e.sequence
        """, turn, after)
        return [(*row[:4], decode_payload(*row[4:])) for row in rows]

    def journal(self, owner):
        return self.read("""
            SELECT json_build_array(id, workspace_id, operation_kind, status, usage_json, started_at, updated_at)
            FROM provider_usage_observation WHERE owner_id = :'arg1'
            ORDER BY started_at, updated_at, id
        """, owner)


def merge(target, update):
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge(target[key], value)
        elif value is not None:
            target[key] = value


def source_evidence(rows):
    evidence = []
    for row in rows:
        for source in row.get('provenance', []):
            if source not in evidence:
                evidence.append(source)
    return evidence


def execution_cohort(row):
    sources = row.get('provenance', [])
    # Collapse first: duplicate surfaces can supply additional owner evidence.
    owners = {(s.get('workspace_id'), s['logical_conversation_id']) for s in sources
              if s.get('workspace_id') and s.get('logical_conversation_id')}
    if len(owners) == 1 and not any(s.get('conversation_evidence') == 'conflicting' for s in sources):
        workspace, owner = next(iter(owners))
        return ('logical_conversation', workspace, owner)
    # Unknown/conflicting relationships never authorize conversation continuity.
    # A turn surface remains more precise than an auxiliary operation owner.
    for source in sources:
        if source.get('thread_id'):
            return ('execution_thread', source.get('workspace_id'), source['thread_id'])
    for source in sources:
        if source.get('owner_id'):
            return ('operation_owner', source.get('workspace_id'), source['owner_id'])
        if source.get('turn_id'):
            return ('selected_turn_owner_unknown', source.get('workspace_id'), source['turn_id'])
    return ('unattributed_sources', None, None)


def turn_conversation_evidence(source, turn, thread, workspace, runtime):
    """Admit retained hook or exact durable Composer launch; never Task ancestry.

    Terminal reconciliation deletes runtime snapshots, but retains Task/run/turn
    records and versioned composerWork. Reads use this report's read-only handle
    and fetch at most two exact bindings before parsing, without a transaction.
    """
    result = dict(workspace_id=workspace, task_id=None, logical_conversation_id=None,
                  conversation_evidence='unavailable')
    owners, sources = set(), []
    conflict = False
    if runtime:
        try:
            hook = json.loads(runtime[2])
        except (ValueError, TypeError):
            hook = None
        if (runtime[1] != thread or not runtime[0] or
                (workspace is not None and runtime[0] != workspace) or not isinstance(hook, dict)):
            conflict = True
        else:
            result['workspace_id'] = runtime[0]
            task = hook.get('task_id')
            owner = hook.get('conversation_thread_id')
            if ((task is not None and (not isinstance(task, str) or not task.strip())) or
                    (owner is not None and not isinstance(owner, str))):
                conflict = True
            else:
                result['task_id'] = task
                owners.add(owner if owner and owner.strip() else thread)
                sources.append('runtime_snapshot')
    rows = source.task_bindings(turn)
    if rows:
        if len(rows) > 1:
            conflict = True
        elif rows:
            (task, execution, task_workspace, owner_kind, owner, creator, origin_turn,
             executor, lifecycle_json, metadata_json, execution_workspace, actual_thread,
             owner_workspace, origin_thread, run_task) = rows[0]
            if (execution != thread or actual_thread != thread or not task_workspace or
                    task_workspace != execution_workspace or run_task != task or
                    (result['workspace_id'] is not None and task_workspace != result['workspace_id']) or
                    (result['task_id'] is not None and result['task_id'] != task)):
                conflict = True
            else:
                result.update(workspace_id=task_workspace, task_id=task)
                try:
                    metadata = json.loads(metadata_json) if metadata_json else {}
                    lifecycle = json.loads(lifecycle_json) if lifecycle_json else {}
                except (ValueError, TypeError):
                    metadata, lifecycle = None, None
                if not isinstance(metadata, dict) or not isinstance(lifecycle, dict):
                    conflict = True
                else:
                    work = metadata.get('composerWork')
                    if work is not None:
                        launch = work.get('launch') if isinstance(work, dict) else None
                        # Match the versioned launch to its original Composer
                        # destination and source turn, not an arbitrary owner_id.
                        if (not isinstance(work, dict) or type(work.get('version')) is not int or
                                work['version'] != 1 or not isinstance(launch, dict) or
                                executor != 'agent' or lifecycle.get('attachment') != 'detached' or
                                owner_kind != 'thread' or not isinstance(owner, str) or not owner.strip() or
                                owner != creator or owner_workspace != task_workspace or
                                origin_thread != owner or not origin_turn or
                                launch.get('thread_id') != owner or launch.get('turn_id') != origin_turn):
                            conflict = True
                        else:
                            owners.add(owner)
                            sources.append('composer_work_v1')
                    elif lifecycle.get('attachment') == 'attached' and owners - {thread}:
                        conflict = True
    if conflict or len(owners) > 1:
        result.update(logical_conversation_id=None, conversation_evidence='conflicting')
    elif owners:
        result.update(logical_conversation_id=next(iter(owners)),
                      conversation_evidence='+'.join(sources))
    return result


def retain_connection(rows, target):
    # One physical dispatch may have a labelled and an older unlabelled surface.
    # Preserve evidence, never split that dispatch into additional paid calls.
    values = set()
    for row in rows:
        values.update(row.get('connection_candidates', []))
        if row.get('connection') is not None:
            values.add(row['connection'])
    target['connection_candidates'] = sorted(values)
    target['connection'] = next(iter(values)) if len(values) == 1 else None


def collapse(snapshots):
    """Replace cumulative updates, preserve physical retries, dedup redelivery."""
    by_observation = defaultdict(list)
    for row in snapshots:
        by_observation[(row['source'], row['id'])].append(row)
    observations = []
    for rows in by_observation.values():
        attempts = {r['usage'].get('physical_attempt_id') for r in rows} - {None, ''}
        buckets = defaultdict(list)
        for row in rows:
            attempt = row['usage'].get('physical_attempt_id')
            if not attempt and len(attempts) == 1:
                attempt = next(iter(attempts))
            buckets[attempt].append(row)
        for attempt, updates in buckets.items():
            updates.sort(key=lambda r: (r['time'], r['sequence']))
            result = dict(updates[-1])
            terminal = [r for r in updates if r['status'] in ('completed', 'failed')]
            if terminal:
                result['status'] = terminal[-1]['status']
            result['first_time'] = min(r.get('first_time', r['time']) for r in updates)
            result['usage'] = {}
            for update in updates:
                merge(result['usage'], update['usage'])
            result['attempt'] = attempt
            result['updates'] = len(updates)
            result['provenance'] = source_evidence(updates)
            retain_connection(updates, result)
            observations.append(result)
    # A physical provider call can be observed through more than one surface.
    unique = {}
    for row in sorted(observations, key=lambda r: (r['time'], r['sequence'])):
        usage = row['usage']
        key = ('physical', usage.get('provider', row.get('provider')), usage.get('route', row.get('route')), row['attempt']) if row['attempt'] else (row['source'], row['id'])
        if key in unique:
            previous = unique[key]
            merged = dict(previous['usage'])
            merge(merged, usage)
            row['usage'] = merged
            row['first_time'] = min(previous['first_time'], row['first_time'])
            row['updates'] += previous['updates']
            row['provenance'] = source_evidence([previous, row])
            retain_connection([previous, row], row)
        unique[key] = row
    return list(unique.values())


def measures(usage):
    read = counter(usage.get('cache_read_input_tokens'))
    write = counter(usage.get('cache_write_input_tokens'))
    uncached = counter(usage.get('uncached_input_tokens'))
    total = counter(usage.get('input_tokens'))
    if usage.get('semantics') == 'aihubmix_claude_input_unresolved':
        # A later native envelope can invalidate a prompt-only partial total.
        # Older snapshot mergers deliberately retain non-null categories.
        total, uncached = None, None
    if usage.get('semantics') in ('exclusive_cache', 'opencode_v2_exclusive_cache_separate_reasoning'):
        total = strict_sum([uncached, read, write])
    return total, read, write, uncached


def cache_observations(usage):
    """Retained native observations, separate from paid writes and hit ratio."""
    raw = usage.get('raw_usage')
    if not isinstance(raw, dict):
        return None
    details = raw.get('prompt_tokens_details')
    details = details if isinstance(details, dict) else {}
    result = {}
    for provider, field, meaning in (
            ('sambanova', 'cache_creation_tokens', 'informational_creation'),
            ('vllm', 'created_cache_tokens', 'observed_creation')):
        if usage.get('provider') != provider:
            continue
        count = counter(details.get(field))
        if count is not None:
            result[field] = dict(tokens=count, semantics=meaning)
    claude = raw.get('claude_cache_tokens_details')
    if usage.get('provider') == 'aihubmix' and isinstance(claude, dict):
        result['claude_cache_tokens_details'] = {
            key: counter(claude.get(key)) for key in (
                'cache_creation_input_tokens', 'cache_read_input_tokens',
                'cache_write_5_minutes_input_tokens', 'cache_write_1_hour_input_tokens')}
        result['prompt_tokens_observed'] = counter(raw.get('prompt_tokens'))
        result['input_semantics'] = usage.get('semantics')
    return result or None


def partial_evidence(usage):
    # Native OpenCode turn details carry this at the top level; isolated
    # summaries retain those same details in TokenUsage.raw_usage.
    raw = usage.get('raw_usage')
    partial = usage.get('known_partial_tokens')
    if not isinstance(partial, dict) and isinstance(raw, dict):
        partial = raw.get('known_partial_tokens')
    if not isinstance(partial, dict):
        return None
    return {key: counter(partial.get(key)) for key in
            ('input', 'output', 'reasoning', 'cache_read', 'cache_write')}


def aggregate(rows):
    groups = defaultdict(list)
    for row in collapse(rows):
        u = row['usage']
        provider = u.get('provider') or row.get('provider')
        mode = 'cli' if provider in ('codex-cli', 'claude-cli', 'opencode-cli') else 'api'
        scope = (row.get('scope') or 'isolated_cli_call') if mode == 'cli' else ('physical_attempt' if row['attempt'] else 'dispatch_unknown')
        identity = (provider, row.get('connection'), u.get('route') or row.get('route'), mode, u.get('api') or row.get('api'), u.get('model') or row.get('model'), row.get('purpose') or 'unknown', scope, execution_cohort(row))
        groups[identity].append(row)
    report = []
    for key, group in sorted(groups.items(), key=lambda item: str(item[0])):
        group.sort(key=lambda r: (r['first_time'], r['sequence']))
        def sample(selected):
            known, excluded = [], []
            writes = []
            for row in selected:
                total, read, write, uncached = measures(row['usage'])
                if write is not None:
                    writes.append(write)
                reason = ('incomplete_status' if row['status'] != 'completed' else
                          'unknown_input_or_read' if total is None or read is None else
                          'invalid_read_gt_input' if read > total else
                          'invalid_write_gt_input' if write is not None and write > total else
                          'invalid_cache_categories_gt_input' if write is not None and read + write > total else
                          'unconfirmed_physical_dispatch' if key[7] == 'dispatch_unknown' else None)
                metrics = dict(id=row['id'], provenance=row.get('provenance', []),
                               connection_candidates=row.get('connection_candidates', []), total_input=total, cache_read=read, cache_write=write, uncached_input=uncached,
                               known_partial_tokens=partial_evidence(row['usage']),
                               cache_observations=cache_observations(row['usage']),
                               comparison_response_id=row['usage'].get('prompt_cache_comparison_response_id'),
                               prompt_cache_diagnostics=row['usage'].get('prompt_cache_diagnostics'))
                if reason:
                    excluded.append(dict(metrics, reason=reason, status=row['status']))
                else:
                    known.append(metrics)
            total = sum(r['total_input'] for r in known)
            read = sum(r['cache_read'] for r in known)
            return dict(observations=len(selected), included=len(known), included_observations=known, excluded=excluded,
                        cache_observations=[dict(id=row['id'], observations=cache_observations(row['usage']))
                                            for row in selected if cache_observations(row['usage'])],
                        diagnostics=[dict(id=row['id'], comparison_response_id=row['usage'].get('prompt_cache_comparison_response_id'),
                                          result=row['usage'].get('prompt_cache_diagnostics')) for row in selected
                                     if row['usage'].get('prompt_cache_comparison_response_id') or row['usage'].get('prompt_cache_diagnostics')], total_input=total,
                        cache_read=read, token_hit_rate=100 * read / total if total else None,
                        cache_write_known_sum=sum(writes) if writes else None, cache_write_known_observations=len(writes))
        report.append(dict(zip(('provider', 'connection', 'endpoint', 'mode', 'api', 'model', 'purpose', 'scope'), key),
                           all=sample(group), first_observed=sample(group[:1]), subsequent=sample(group[1:]),
                           connection_known=key[1] is not None,
                           connection_scope='confirmed_authority' if key[1] is not None else 'unattributed; account continuity unknown',
                           execution_cohort=dict(zip(('kind', 'workspace_id', 'owner_id'), key[8]))))
    return report


def load(source, turns, owners, purposes):
    snapshots, cumulative = [], {}
    unavailable_cli_updates = 0
    for turn in turns:
        runtime = source.runtime_snapshot(turn)
        relationships = {}
        # Fetch bounded batches; decoding never holds a DB transaction/stream.
        after = 0
        turn_completed = False
        first_snapshot = len(snapshots)
        while True:
            # sequence is the immutable per-turn cursor.
            rows = source.event_page(turn, after)
            if not rows:
                break
            after = rows[-1][2]
            for event_id, thread, sequence, created, payload in rows:
                event = json.loads(payload)
                if event.get('kind') == 'turn_completed' and event.get('payload', {}).get('turn', {}).get('status') == 'completed':
                    turn_completed = True
                notification = event.get('payload', {})
                workspace = notification.get('workspace_id')
                identity = (thread, workspace)
                if identity not in relationships:
                    relationships[identity] = turn_conversation_evidence(
                        source, turn, thread, workspace, runtime)
                provenance = dict(source='turn_event', thread_id=thread, turn_id=turn, owner_id=thread,
                                  **relationships[identity])
                item = notification.get('item', {})
                details = item.get('details') or {}
                if not isinstance(details, dict):
                    details = {}
                if item.get('code') == 'provider_usage':
                    observation = details.get('observation_id') or item['id']
                    usage = details.get('usage')
                    snapshots.append(dict(source='turn_event', id=observation, time=timestamp(created), sequence=sequence,
                        purpose=details.get('purpose') or purposes.get(observation), status=details.get('status', 'unknown'),
                        provider=details.get('provider'), model=details.get('model'), api=details.get('api'), route=details.get('route'),
                        connection=details.get('connection_id'), provenance=[dict(provenance, observation_id=observation,
                            connection_id=details.get('connection_id'))],
                        usage=usage if isinstance(usage, dict) else {}))
                elif item.get('code') == 'agent_runtime_event' and details.get('nativeMethod') == 'thread/tokenUsage/updated':
                    native = details.get('nativeThreadId')
                    # No native identity means no safe cumulative replacement.
                    identity = hashlib.sha256(native.encode()).hexdigest() if isinstance(native, str) and native else event_id
                    native_payload = details.get('payload')
                    retained = native_payload.get('tokenUsage') if isinstance(native_payload, dict) else None
                    total = retained.get('total') if isinstance(retained, dict) else None
                    if isinstance(total, dict):
                        total = {key:counter(total.get(key)) for key in ('inputTokens', 'outputTokens', 'cachedInputTokens', 'reasoningOutputTokens', 'totalTokens')}
                        candidate = dict(session_identity=identity, time=timestamp(created), totals=total,
                            reason='cumulative_session_total; selected-turn baseline/continuity unknown',
                            provenance=[dict(provenance, source='cli_session', observation_id=identity)])
                        if identity in cumulative:
                            candidate['provenance'] = source_evidence([cumulative[identity], candidate])
                        if identity not in cumulative or candidate['time'] >= cumulative[identity]['time']:
                            cumulative[identity] = candidate
                        else:
                            cumulative[identity]['provenance'] = candidate['provenance']
                    else:
                        unavailable_cli_updates += 1
                elif (item.get('code') == 'agent_runtime_item' and details.get('nativeItemKind') == 'tokenUsage'
                      and details.get('semantics') == 'opencode_v2_exclusive_cache_separate_reasoning'):
                    # One cumulative turn summary, not one charge per update or
                    # a known count of internal physical provider attempts.
                    usage = dict(details, provider='opencode-cli', api='opencode_v2')
                    usage['cache_write_input_tokens'] = details.get('cache_creation_input_tokens')
                    snapshots.append(dict(source='cli_turn', id=turn, time=timestamp(created), sequence=sequence,
                                          purpose='main', status='partial', scope='cli_turn_total', provenance=[dict(provenance, source='cli_turn', observation_id=turn)], usage=usage))
        if turn_completed:
            for snapshot in snapshots[first_snapshot:]:
                if snapshot['source'] == 'cli_turn':
                    snapshot['status'] = 'completed'
    for owner in sorted(set(turns + owners)):
        for row in source.journal(owner):
            ident, workspace, purpose, status, payload, started, updated = row
            usage = json.loads(payload)
            metadata = usage.get('_pioneer_journal') if isinstance(usage, dict) else None
            authority = metadata.get('connection_id') if isinstance(metadata, dict) and metadata.get('schema_version') == 1 else None
            if not isinstance(authority, str) or not authority or len(authority.encode('utf-8')) > 256:
                authority = None
            snapshots.append(dict(source='operation_journal', id=ident, purpose=purpose, status=status, usage=usage if isinstance(usage, dict) else {},
                                  connection=authority,
                                  time=float(updated), first_time=float(started), sequence=0,
                                  provenance=[dict(source='operation_journal', observation_id=ident,
                                                   workspace_id=workspace, owner_id=owner, connection_id=authority)]))
    return dict(groups=aggregate(snapshots), snapshots=len(snapshots), cumulative_cli=list(cumulative.values()),
                unavailable_cli_usage_updates=unavailable_cli_updates,
                notes=['First observed is within the retained execution cohort; it does not prove a cold provider cache.',
                       'Confirmed logical conversation owns first/subsequent; actual execution sources remain visible. Unknown/conflicting relationships use execution fallback.',
                       'Absent connection identity is unknown; endpoint identity does not identify an account.',
                       'Failed/partial/unknown dispatch observations are excluded from ratio; known counters remain visible.',
                       'Native CLI session totals are not added to physical API attempts.',
                       'Cost and TTFT comparison unavailable in this token report.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-home', required=True, help='runtime_home from pioneer status --json')
    parser.add_argument('--gateway', default=shutil.which("pioneer"), help='path to installed pioneer')
    parser.add_argument('--turn', action='append', default=[])
    parser.add_argument('--owner', action='append', default=[])
    parser.add_argument('--purpose', action='append', default=[], metavar='OBSERVATION=PURPOSE')
    args = parser.parse_args()
    if not args.turn and not args.owner:
        parser.error('select at least one --turn or --owner')
    purposes = dict(value.split('=', 1) for value in args.purpose)
    if not args.gateway:
        parser.error('provide --gateway or put the installed pioneer on PATH')
    source = GatewayPostgres(args.gateway, args.runtime_home)
    result = load(source, args.turn, args.owner, purposes)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
