#!/usr/bin/env python3
"""Read retained usage only. No Pioneer imports, model calls, or database writes.

Use --turn for turn events, --owner for service-operation journal owners. Old
purpose attribution is explicit (--purpose OBSERVATION_ID=PURPOSE), never guessed
from sequence position. Codex session totals are separate, never turn charges.
"""
import argparse
import ctypes
import ctypes.util
import datetime
import hashlib
import json
import pathlib
import sqlite3
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


class Decoder:
    """Same compact zstd format as sqlite/src/zstd/basic.rs, including dicts."""
    def __init__(self, connection):
        self.connection = connection
        self.lib = None

    def decode(self, payload, dictionary_id):
        if dictionary_id is None:
            return payload.decode('utf-8') if isinstance(payload, bytes) else payload
        if self.lib is None:
            self.lib = ctypes.CDLL(ctypes.util.find_library('zstd'))
            self.lib.ZSTD_createDCtx.restype = ctypes.c_void_p
            self.lib.ZSTD_freeDCtx.argtypes = [ctypes.c_void_p]
            self.lib.ZSTD_decompress_usingDict.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
            self.lib.ZSTD_decompress_usingDict.restype = ctypes.c_size_t
            self.lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
            self.lib.ZSTD_isError.restype = ctypes.c_uint
        dictionary = b'' if dictionary_id == -1 else self.connection.execute(
            'SELECT dict FROM _zstd_dicts WHERE id=?', (dictionary_id,)
        ).fetchone()[0]
        frame = b'\x28\xb5\x2f\xfd' + payload
        # Usage events are small. Fail explicitly instead of silently truncating
        # or retaining an unbounded decoded conversation record.
        output = ctypes.create_string_buffer(8 * 1024 * 1024)
        context = self.lib.ZSTD_createDCtx()
        if not context:
            raise RuntimeError('zstd context allocation failed')
        try:
            size = self.lib.ZSTD_decompress_usingDict(context, output, len(output), frame, len(frame), dictionary, len(dictionary))
            if self.lib.ZSTD_isError(size):
                raise ValueError('cannot decode compact zstd event (8 MiB limit)')
            return output.raw[:size].decode('utf-8')
        finally:
            self.lib.ZSTD_freeDCtx(context)


def merge(target, update):
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge(target[key], value)
        elif value is not None:
            target[key] = value


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
        unique[key] = row
    return list(unique.values())


def measures(usage):
    read = counter(usage.get('cache_read_input_tokens'))
    write = counter(usage.get('cache_write_input_tokens'))
    uncached = counter(usage.get('uncached_input_tokens'))
    total = counter(usage.get('input_tokens'))
    if usage.get('semantics') in ('exclusive_cache', 'opencode_v2_exclusive_cache_separate_reasoning'):
        total = strict_sum([uncached, read, write])
    return total, read, write, uncached


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
        identity = (provider, row.get('connection'), u.get('route') or row.get('route'), mode, u.get('api') or row.get('api'), u.get('model') or row.get('model'), row.get('purpose') or 'unknown', scope)
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
                          'unconfirmed_physical_dispatch' if key[-1] == 'dispatch_unknown' else None)
                metrics = dict(id=row['id'], total_input=total, cache_read=read, cache_write=write, uncached_input=uncached,
                               known_partial_tokens=partial_evidence(row['usage']),
                               comparison_response_id=row['usage'].get('prompt_cache_comparison_response_id'),
                               prompt_cache_diagnostics=row['usage'].get('prompt_cache_diagnostics'))
                if reason:
                    excluded.append(dict(metrics, reason=reason, status=row['status']))
                else:
                    known.append(metrics)
            total = sum(r['total_input'] for r in known)
            read = sum(r['cache_read'] for r in known)
            return dict(observations=len(selected), included=len(known), excluded=excluded,
                        diagnostics=[dict(id=row['id'], comparison_response_id=row['usage'].get('prompt_cache_comparison_response_id'),
                                          result=row['usage'].get('prompt_cache_diagnostics')) for row in selected
                                     if row['usage'].get('prompt_cache_comparison_response_id') or row['usage'].get('prompt_cache_diagnostics')], total_input=total,
                        cache_read=read, token_hit_rate=100 * read / total if total else None,
                        cache_write_known_sum=sum(writes) if writes else None, cache_write_known_observations=len(writes))
        report.append(dict(zip(('provider', 'connection', 'endpoint', 'mode', 'api', 'model', 'purpose', 'scope'), key),
                           all=sample(group), first_observed=sample(group[:1]), subsequent=sample(group[1:]),
                           connection_known=key[1] is not None))
    return report


def load(connection, turns, owners, purposes):
    decoder = Decoder(connection)
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
    snapshots, cumulative = [], {}
    unavailable_cli_updates = 0
    for turn in turns:
        sql = ('SELECT id,sequence,created_at,payload,_payload_dict FROM _turn_event_zstd WHERE turn_id=? ORDER BY sequence'
               if '_turn_event_zstd' in tables else
               'SELECT id,sequence,created_at,payload,NULL FROM turn_event WHERE turn_id=? ORDER BY sequence')
        # Fetch bounded batches; decoding never holds a DB transaction/stream.
        after = 0
        turn_completed = False
        first_snapshot = len(snapshots)
        while True:
            # sequence is the immutable per-turn cursor.
            rows = connection.execute(sql.replace('ORDER BY sequence', 'AND sequence>? ORDER BY sequence LIMIT 128'), (turn, after)).fetchall()
            if not rows:
                break
            after = rows[-1][1]
            for event_id, sequence, created, payload, dictionary_id in rows:
                event = json.loads(decoder.decode(payload, dictionary_id))
                if event.get('kind') == 'turn_completed' and event.get('payload', {}).get('turn', {}).get('status') == 'completed':
                    turn_completed = True
                item = event.get('payload', {}).get('item', {})
                details = item.get('details') or {}
                if not isinstance(details, dict):
                    details = {}
                if item.get('code') == 'provider_usage':
                    observation = details.get('observation_id') or item['id']
                    usage = details.get('usage')
                    snapshots.append(dict(source='turn_event', id=observation, time=timestamp(created), sequence=sequence,
                        purpose=details.get('purpose') or purposes.get(observation), status=details.get('status', 'unknown'),
                        provider=details.get('provider'), model=details.get('model'), api=details.get('api'), route=details.get('route'),
                        connection=details.get('connection_id'), usage=usage if isinstance(usage, dict) else {}))
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
                            reason='cumulative_session_total; selected-turn baseline/continuity unknown')
                        if identity not in cumulative or candidate['time'] >= cumulative[identity]['time']:
                            cumulative[identity] = candidate
                    else:
                        unavailable_cli_updates += 1
                elif (item.get('code') == 'agent_runtime_item' and details.get('nativeItemKind') == 'tokenUsage'
                      and details.get('semantics') == 'opencode_v2_exclusive_cache_separate_reasoning'):
                    # One cumulative turn summary, not one charge per update or
                    # a known count of internal physical provider attempts.
                    usage = dict(details, provider='opencode-cli', api='opencode_v2')
                    usage['cache_write_input_tokens'] = details.get('cache_creation_input_tokens')
                    snapshots.append(dict(source='cli_turn', id=turn, time=timestamp(created), sequence=sequence,
                                          purpose='main', status='partial', scope='cli_turn_total', usage=usage))
        if turn_completed:
            for snapshot in snapshots[first_snapshot:]:
                if snapshot['source'] == 'cli_turn':
                    snapshot['status'] = 'completed'
    if 'provider_usage_observation' in tables:
        for owner in sorted(set(turns + owners)):
            for row in connection.execute('SELECT id,operation_kind,status,usage_json,started_at,updated_at FROM provider_usage_observation WHERE owner_id=?', (owner,)).fetchall():
                ident, purpose, status, payload, started, updated = row
                usage = json.loads(payload)
                snapshots.append(dict(source='operation_journal', id=ident, purpose=purpose, status=status, usage=usage if isinstance(usage, dict) else {},
                                      time=float(updated), first_time=float(started), sequence=0))
    return dict(groups=aggregate(snapshots), snapshots=len(snapshots), cumulative_cli=list(cumulative.values()),
                unavailable_cli_usage_updates=unavailable_cli_updates,
                notes=['First observed does not prove a cold provider cache.',
                       'Absent connection identity is unknown; endpoint identity does not identify an account.',
                       'Failed/partial/unknown dispatch observations are excluded from ratio; known counters remain visible.',
                       'Native CLI session totals are not added to physical API attempts.',
                       'Cost and TTFT comparison unavailable in this token report.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('--turn', action='append', default=[])
    parser.add_argument('--owner', action='append', default=[])
    parser.add_argument('--purpose', action='append', default=[], metavar='OBSERVATION=PURPOSE')
    args = parser.parse_args()
    if not args.turn and not args.owner:
        parser.error('select at least one --turn or --owner')
    purposes = dict(value.split('=', 1) for value in args.purpose)
    uri = pathlib.Path(args.db).resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as connection:
        connection.execute('PRAGMA query_only=ON')
        result = load(connection, args.turn, args.owner, purposes)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
