"""Deferred tests; do not run before explicit external acceptance."""
import unittest
from prompt_cache_report import aggregate, collapse, measures, timestamp


def row(ident, total=100, read=80, status='completed', attempt=None, **metadata):
    result = dict(source='turn_event', id=ident, time=1, sequence=1, status=status,
                  purpose='main', usage=dict(provider='openai', api='responses', model='fixture',
                  input_tokens=total, cache_read_input_tokens=read, physical_attempt_id=attempt or ident))
    result.update(metadata)
    return result


class CacheReportTests(unittest.TestCase):
    def test_provider_diagnostics_remain_separate_from_measured_cache_ratio(self):
        record = row('observed', total=100, read=80)
        record['usage']['prompt_cache_comparison_response_id'] = 'resp_baseline'
        record['usage']['prompt_cache_diagnostics'] = dict(
            type='cache_miss', reason='input_changed', cache_missed_tokens=999)
        group, = aggregate([record])
        self.assertEqual(group['all']['token_hit_rate'], 80)
        self.assertEqual(group['all']['diagnostics'], [dict(
            id='observed', comparison_response_id='resp_baseline',
            result=record['usage']['prompt_cache_diagnostics'])])

    def test_load_opencode_summary_raw_envelope_and_native_turn_partial_evidence(self):
        import json
        import sqlite3
        from prompt_cache_report import load
        details = dict(nativeItemKind='tokenUsage',
                       semantics='opencode_v2_exclusive_cache_separate_reasoning',
                       uncached_input_tokens=30, cache_read_input_tokens=None,
                       cache_creation_input_tokens=0,
                       known_partial_tokens=dict(input=30, cache_read=80, cache_write=0,
                                                 output=True, reasoning=-1))
        # Shape written by apply_cli_usage -> provider_usage_observation;
        # the native details are retained under TokenUsage.raw_usage.
        usage = dict(provider='opencode-cli', api='opencode_v2', model='fixture',
                     semantics=details['semantics'], input_tokens=None,
                     uncached_input_tokens=30, cache_read_input_tokens=None,
                     cache_write_input_tokens=0, raw_usage=details)
        with sqlite3.connect(':memory:') as connection:
            connection.execute('CREATE TABLE provider_usage_observation(id TEXT,owner_id TEXT,operation_kind TEXT,status TEXT,usage_json TEXT,started_at REAL,updated_at REAL)')
            connection.execute('INSERT INTO provider_usage_observation VALUES (?,?,?,?,?,?,?)',
                               ('summary', 'operation', 'compaction', 'completed', json.dumps(usage), 1, 2))
            connection.execute('CREATE TABLE turn_event(id TEXT,turn_id TEXT,sequence INTEGER,created_at TEXT,payload TEXT)')
            event = dict(kind='item_completed', payload=dict(item=dict(
                code='agent_runtime_item', details=details)))
            connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?)',
                               ('native', 'turn', 1, '2026-01-01T00:00:00Z', json.dumps(event)))
            connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?)',
                               ('terminal', 'turn', 2, '2026-01-01T00:00:01Z', json.dumps(
                                   dict(kind='turn_completed', payload=dict(turn=dict(status='completed'))))))
            report = load(connection, ['turn'], ['operation'], {})
        self.assertEqual(len(report['groups']), 2)
        self.assertEqual({g['scope'] for g in report['groups']},
                         {'isolated_cli_call', 'cli_turn_total'})
        for group in report['groups']:
            sample = group['all']
            self.assertEqual(sample['included'], 0)
            self.assertIsNone(sample['token_hit_rate'])
            self.assertEqual(sample['cache_read'], 0)  # no partial evidence in numerator
            evidence, = sample['excluded']
            self.assertEqual(evidence['reason'], 'unknown_input_or_read')
            self.assertIsNone(evidence['total_input'])
            self.assertIsNone(evidence['cache_read'])
            self.assertEqual(evidence['known_partial_tokens']['cache_read'], 80)
            self.assertEqual(evidence['known_partial_tokens']['cache_write'], 0)
            self.assertIsNone(evidence['known_partial_tokens']['output'])
            self.assertIsNone(evidence['known_partial_tokens']['reasoning'])

    def test_ratio_of_sums_and_first_observed_are_separate(self):
        first = row('first', 100, 0, time=1)
        second = row('second', 900, 900, time=2)
        group, = aggregate([first, second])
        self.assertEqual(group['all']['token_hit_rate'], 90)
        self.assertEqual(group['first_observed']['token_hit_rate'], 0)
        self.assertEqual(group['subsequent']['token_hit_rate'], 100)
        self.assertFalse(group['connection_known'])

    def test_exclusive_total_requires_each_category_and_writes_are_not_hits(self):
        usage = dict(semantics='exclusive_cache', input_tokens=10, uncached_input_tokens=10,
                     cache_read_input_tokens=80, cache_write_input_tokens=10)
        self.assertEqual(measures(usage), (100, 80, 10, 10))
        del usage['cache_write_input_tokens']
        self.assertEqual(measures(usage), (None, 80, None, 10))
        usage['semantics'] = 'inclusive_cache_and_reasoning'
        usage['input_tokens'] = 100
        self.assertEqual(measures(usage)[:2], (100, 80))

    def test_unknown_zero_partial_error_and_bad_evidence_are_visible(self):
        unknown = row('unknown', None, 10)
        error = row('failed', 30, 20, status='failed')
        invalid = row('invalid', 10, 11)
        local = row('local', None, None)
        local['usage']['physical_attempt_id'] = None
        groups = aggregate([row('zero', 0, 0), unknown, error, invalid, local])
        physical = next(group for group in groups if group['scope']=='physical_attempt')
        self.assertIsNone(physical['all']['token_hit_rate'])
        self.assertEqual(physical['all']['included'], 1)
        partial = {part['id']:part for part in physical['all']['excluded']}
        self.assertEqual(partial['failed']['cache_read'], 20)
        self.assertEqual(partial['invalid']['reason'], 'invalid_read_gt_input')
        self.assertIsNone(partial['unknown']['total_input'])
        local_group = next(group for group in groups if group['scope']=='dispatch_unknown')
        self.assertEqual(local_group['all']['included'], 0)

    def test_streaming_updates_redelivery_and_physical_retries(self):
        start = row('observation', None, None, status='started', time=1)
        start['usage']['physical_attempt_id'] = None
        end = row('observation', 100, 80, attempt='attempt-1', time=2)
        retry = row('observation', 120, 90, attempt='attempt-2', time=3)
        records = collapse([start, end, end, retry])
        physical = [record for record in records if record['attempt']]
        self.assertEqual(len(physical), 2)
        self.assertEqual(sum(record['usage']['input_tokens'] for record in physical), 220)
        # Another surface delivering the same physical attempt is not a charge.
        journal = row('journal', 100, 80, attempt='attempt-1', source='operation_journal', time=4)
        self.assertEqual(len(collapse([end, journal])), 1)

    def test_partial_updates_preserve_previously_known_categories(self):
        initial = row('observation')
        final = row('observation', time=2)
        final['usage'] = dict(physical_attempt_id='observation', output_tokens=10)
        observation, = collapse([initial, final])
        self.assertEqual(measures(observation['usage'])[:2], (100, 80))

    def test_preflight_and_isolated_cli_totals_never_mix_with_main_api(self):
        cli = row('cli', 200, 150, source='operation_journal', purpose='compaction')
        cli['usage'].update(provider='claude-cli', api='claude_print', physical_attempt_id=None)
        cli_update = row('cli', 300, 250, source='operation_journal', purpose='compaction', time=2)
        cli_update['usage'].update(provider='claude-cli', api='claude_print', physical_attempt_id=None)
        groups = aggregate([row('main'), row('preflight', purpose='preflight'), cli, cli_update])
        self.assertEqual(len(groups), 3)
        cli_group = next(group for group in groups if group['mode']=='cli')
        self.assertEqual(cli_group['scope'], 'isolated_cli_call')
        self.assertEqual(cli_group['all']['total_input'], 300)
        self.assertEqual(cli_group['all']['observations'], 1)
        # The word cli in an API name does not make it a CLI execution.
        api = row('api'); api['usage']['api'] = 'public_api'
        self.assertEqual(aggregate([api])[0]['mode'], 'api')

    def test_numeric_time_and_utc_source_time_share_one_order(self):
        self.assertEqual(timestamp(1), timestamp('1970-01-01T00:00:01Z'))
        self.assertEqual(timestamp('1970-01-01 00:00:01'), 1)
        self.assertEqual(timestamp('1970-01-01T03:00:01+03:00'), 1)

    def test_native_cli_cumulative_updates_have_no_invented_turn_baseline(self):
        import json
        import sqlite3
        from prompt_cache_report import load
        with sqlite3.connect(':memory:') as connection:
            connection.execute('CREATE TABLE turn_event(id TEXT,turn_id TEXT,sequence INTEGER,created_at TEXT,payload TEXT)')
            for sequence, total in [(1, 100), (2, 150), (3, 150)]:
                event = dict(kind='item_completed', payload=dict(item=dict(code='agent_runtime_event', details=dict(
                    nativeMethod='thread/tokenUsage/updated', nativeThreadId='native-session',
                    payload=dict(tokenUsage=dict(total=dict(inputTokens=total, cachedInputTokens=80, outputTokens=10)))))))
                connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?)', (str(sequence), 'turn', sequence, '2026-01-01T00:00:00Z', json.dumps(event)))
            report = load(connection, ['turn'], [], {})
        self.assertEqual(report['groups'], [])
        self.assertEqual(len(report['cumulative_cli']), 1)
        self.assertEqual(report['cumulative_cli'][0]['totals']['inputTokens'], 150)
        self.assertIn('baseline/continuity unknown', report['cumulative_cli'][0]['reason'])
