"""Deferred tests; do not run before explicit external acceptance."""
import unittest
from prompt_cache_report import aggregate, collapse, measures, timestamp


def row(ident, total=100, read=80, status='completed', attempt=None, **metadata):
    result = dict(source='turn_event', id=ident, time=1, sequence=1, status=status,
                  purpose='main', usage=dict(provider='openai', api='responses', model='fixture',
                  input_tokens=total, cache_read_input_tokens=read, physical_attempt_id=attempt or ident))
    result.update(metadata)
    result.setdefault('provenance', [dict(source=result['source'], workspace_id='fixture-workspace',
                                        thread_id='fixture-thread', turn_id='fixture-turn', observation_id=ident)])
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
            connection.execute('CREATE TABLE provider_usage_observation(id TEXT,workspace_id TEXT,owner_id TEXT,operation_kind TEXT,status TEXT,usage_json TEXT,started_at REAL,updated_at REAL)')
            connection.execute('INSERT INTO provider_usage_observation VALUES (?,?,?,?,?,?,?,?)',
                               ('summary', 'fixture-workspace', 'operation', 'compaction', 'completed', json.dumps(usage), 1, 2))
            connection.execute('CREATE TABLE turn_event(id TEXT,thread_id TEXT,turn_id TEXT,sequence INTEGER,created_at TEXT,payload TEXT)')
            event = dict(kind='item_completed', payload=dict(item=dict(
                code='agent_runtime_item', details=details)))
            connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)',
                               ('native', 'thread', 'turn', 1, '2026-01-01T00:00:00Z', json.dumps(event)))
            connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)',
                               ('terminal', 'thread', 'turn', 2, '2026-01-01T00:00:01Z', json.dumps(
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
        end['connection'] = 'authority'
        observation, = collapse([end, journal])
        self.assertEqual(observation['connection'], 'authority')
        self.assertEqual({s['source'] for s in observation['provenance']}, {'turn_event', 'operation_journal'})

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
            connection.execute('CREATE TABLE turn_event(id TEXT,thread_id TEXT,turn_id TEXT,sequence INTEGER,created_at TEXT,payload TEXT)')
            for sequence, total in [(1, 100), (2, 150), (3, 150)]:
                event = dict(kind='item_completed', payload=dict(item=dict(code='agent_runtime_event', details=dict(
                    nativeMethod='thread/tokenUsage/updated', nativeThreadId='native-session',
                    payload=dict(tokenUsage=dict(total=dict(inputTokens=total, cachedInputTokens=80, outputTokens=10)))))))
                connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)', (str(sequence), 'thread', 'turn', sequence, '2026-01-01T00:00:00Z', json.dumps(event)))
            report = load(connection, ['turn'], [], {})
        self.assertEqual(report['groups'], [])
        self.assertEqual(len(report['cumulative_cli']), 1)
        self.assertEqual(report['cumulative_cli'][0]['totals']['inputTokens'], 150)
        self.assertIn('baseline/continuity unknown', report['cumulative_cli'][0]['reason'])

    def test_retained_native_cache_observations_do_not_become_paid_writes_or_hits(self):
        import json
        import sqlite3
        from prompt_cache_report import load
        records = [
            dict(provider='sambanova', input_tokens=100, cache_read_input_tokens=50,
                 raw_usage=dict(prompt_tokens_details=dict(cache_creation_tokens=50))),
            dict(provider='vllm', input_tokens=100, cache_read_input_tokens=50,
                 raw_usage=dict(prompt_tokens_details=dict(created_cache_tokens=50))),
            dict(provider='aihubmix', input_tokens=22, cache_read_input_tokens=6266,
                 cache_write_input_tokens=0, semantics='aihubmix_claude_input_unresolved',
                 raw_usage=dict(prompt_tokens=22, claude_cache_tokens_details=dict(
                     cache_read_input_tokens=6266, cache_creation_input_tokens=0,
                     cache_write_5_minutes_input_tokens=0, cache_write_1_hour_input_tokens=0))),
        ]
        with sqlite3.connect(':memory:') as connection:
            connection.execute('CREATE TABLE provider_usage_observation(id TEXT,workspace_id TEXT,owner_id TEXT,operation_kind TEXT,status TEXT,usage_json TEXT,started_at REAL,updated_at REAL)')
            for index, usage in enumerate(records):
                usage.update(physical_attempt_id=str(index), api='chat_completions', model='fixture')
                connection.execute('INSERT INTO provider_usage_observation VALUES (?,?,?,?,?,?,?,?)',
                                   (str(index), 'fixture-workspace', 'operation', 'main', 'completed', json.dumps(usage), 1, 2))
            report = load(connection, [], ['operation'], {})
        groups = {group['provider']: group['all'] for group in report['groups']}
        for provider, field, meaning in [('sambanova', 'cache_creation_tokens', 'informational_creation'),
                                         ('vllm', 'created_cache_tokens', 'observed_creation')]:
            self.assertEqual(groups[provider]['token_hit_rate'], 50)
            self.assertIsNone(groups[provider]['cache_write_known_sum'])
            observation, = groups[provider]['cache_observations']
            self.assertEqual(observation['observations'][field], dict(tokens=50, semantics=meaning))
        claude = groups['aihubmix']
        self.assertEqual(claude['included'], 0)
        self.assertIsNone(claude['token_hit_rate'])
        excluded, = claude['excluded']
        self.assertIsNone(excluded['total_input'])
        self.assertEqual(excluded['cache_read'], 6266)
        self.assertEqual(excluded['cache_observations']['claude_cache_tokens_details']['cache_write_1_hour_input_tokens'], 0)
        self.assertEqual(claude['cache_write_known_sum'], 0)

    def test_unresolved_claude_later_envelope_invalidates_early_prompt_total(self):
        initial = row('partial', total=22, read=None, status='started')
        initial['usage']['provider'] = 'aihubmix'
        final = row('partial', total=None, read=6266, time=2)
        final['usage'].update(provider='aihubmix', semantics='aihubmix_claude_input_unresolved')
        group, = aggregate([initial, final, final])
        self.assertEqual(group['all']['observations'], 1)
        self.assertEqual(group['all']['included'], 0)
        evidence, = group['all']['excluded']
        self.assertIsNone(evidence['total_input'])
        self.assertEqual(evidence['cache_read'], 6266)

    def test_paid_write_range_conflicts_are_visible_and_excluded_from_hit_sample(self):
        write_gt = row('write_gt', total=100, read=0)
        write_gt['usage']['cache_write_input_tokens'] = 101
        sum_gt = row('sum_gt', total=100, read=80)
        sum_gt['usage']['cache_write_input_tokens'] = 30
        overflow = row('overflow', total=2**64 - 1, read=2**64 - 1)
        overflow['usage']['cache_write_input_tokens'] = 1
        group, = aggregate([write_gt, sum_gt, overflow, row('partial_write_unknown')])
        self.assertEqual(group['all']['included'], 1)
        self.assertEqual(group['all']['token_hit_rate'], 80)
        excluded = {r['id']: r for r in group['all']['excluded']}
        self.assertEqual(excluded['write_gt']['reason'], 'invalid_write_gt_input')
        self.assertEqual(excluded['sum_gt']['reason'], 'invalid_cache_categories_gt_input')
        self.assertEqual(excluded['overflow']['reason'], 'invalid_cache_categories_gt_input')
        self.assertEqual(excluded['sum_gt']['cache_write'], 30)

    def test_persisted_parent_and_child_executions_keep_separate_cohorts_and_sources(self):
        import json
        import sqlite3
        from prompt_cache_report import load
        # Composer uses distinct hidden executions/Tasks for ordinary messages.
        # Both storage states must preserve the same logical first/subsequent.
        for storage in ['retained_snapshot', 'terminal_cleanup']:
            with self.subTest(storage=storage), sqlite3.connect(':memory:') as connection:
                connection.execute('CREATE TABLE turn_event(id TEXT,thread_id TEXT,turn_id TEXT,sequence INTEGER,created_at REAL,payload TEXT)')
                connection.execute('CREATE TABLE turn_runtime_snapshot(turn_id TEXT,workspace_id TEXT,thread_id TEXT,hook_runtime_context_json TEXT)')
                connection.execute('CREATE TABLE thread(id TEXT PRIMARY KEY,workspace_id TEXT)')
                connection.execute('CREATE TABLE turn(id TEXT PRIMARY KEY,thread_id TEXT)')
                connection.execute('CREATE TABLE task(id TEXT PRIMARY KEY,workspace_id TEXT,owner_kind TEXT,owner_id TEXT,created_by_thread_id TEXT,created_by_turn_id TEXT,executor_kind TEXT,lifecycle_policy_json TEXT,metadata_json TEXT)')
                connection.execute('CREATE TABLE task_run(id TEXT PRIMARY KEY,task_id TEXT)')
                connection.execute('CREATE TABLE task_run_turn(task_id TEXT,run_id TEXT,thread_id TEXT,turn_id TEXT UNIQUE)')
                connection.execute('INSERT INTO thread VALUES (?,?)', ('conversation', 'workspace'))
                negative = ['unknown', 'owner-only', 'foreign-workspace', 'wrong-binding',
                            'conflicting-launch', 'unsupported-version', 'conflicting-snapshot']
                executions = ['parent-a', 'parent-b', 'child'] + negative
                for name in executions:
                    thread, turn, task = name+'-exec', name+'-turn', name+'-task'
                    connection.execute('INSERT INTO thread VALUES (?,?)', (thread, 'workspace'))
                    connection.execute('INSERT INTO turn VALUES (?,?)', (turn, thread))
                    if name == 'unknown':
                        continue
                    origin = name+'-composer-turn'
                    connection.execute('INSERT INTO turn VALUES (?,?)', (origin, 'conversation'))
                    work = dict(version=1, launch=dict(thread_id='conversation', turn_id=origin))
                    metadata = dict(labels=['composer'], composerWork=work)
                    attachment, workspace, binding_thread = 'detached', 'workspace', thread
                    if name == 'child':
                        # Same Task owner/parent destination cannot merge an
                        # attached child whose effective owner is its execution.
                        metadata, attachment = {}, 'attached'
                    elif name == 'owner-only':
                        metadata = {}
                    elif name == 'foreign-workspace':
                        workspace = 'other-workspace'
                    elif name == 'wrong-binding':
                        binding_thread = 'parent-a-exec'
                    elif name == 'conflicting-launch':
                        work['launch']['thread_id'] = 'another-conversation'
                    elif name == 'unsupported-version':
                        work['version'] = 2
                    connection.execute('INSERT INTO task VALUES (?,?,?,?,?,?,?,?,?)',
                        (task, workspace, 'thread', 'conversation', 'conversation', origin,
                         'agent', json.dumps(dict(attachment=attachment)), json.dumps(metadata)))
                    connection.execute('INSERT INTO task_run VALUES (?,?)', (name+'-run', task))
                    connection.execute('INSERT INTO task_run_turn VALUES (?,?,?,?)',
                                       (task, name+'-run', binding_thread, turn))
                    if name in ['parent-a', 'parent-b', 'child', 'conflicting-snapshot']:
                        hook = dict(task_id=task, conversation_thread_id=(
                            None if name == 'child' else 'different-owner' if name == 'conflicting-snapshot' else 'conversation'))
                        connection.execute('INSERT INTO turn_runtime_snapshot VALUES (?,?,?,?)',
                                           (turn, 'workspace', thread, json.dumps(hook)))
                if storage == 'terminal_cleanup':
                    # Real successful reconciliation removes runtime snapshots;
                    # exact Task/run/Composer records survive, not a prefilled cohort.
                    connection.execute("DELETE FROM turn_runtime_snapshot WHERE turn_id IN ('parent-a-turn','parent-b-turn','child-turn')")
                calls = [('parent-a', 'p1', 100, 80, 'authority'),
                         ('parent-b', 'p2', 200, 180, 'authority'),
                         ('child', 'c1', 200, 0, 'authority'),
                         ('child', 'c2', 300, 200, 'authority'),
                         ('parent-b', 'rotated', 50, 0, 'other-authority'),
                         ('parent-b', 'legacy', 50, 0, None)]
                calls += [(name, name, 100, 0, 'authority') for name in negative]
                for time, (name, ident, total, read, authority) in enumerate(calls, 1):
                    turn, thread = name+'-turn', name+'-exec'
                    usage = dict(provider='openai', api='responses', model='fixture', route='route',
                                 physical_attempt_id=ident, input_tokens=total, cache_read_input_tokens=read)
                    details = dict(observation_id=ident, purpose='main', connection_id=authority,
                                   status='completed', usage=usage)
                    notification = dict(workspace_id='workspace', thread_id=thread, turn_id=turn,
                                        item=dict(id=ident, code='provider_usage', details=details))
                    event = dict(kind='item_completed', payload=notification)
                    for suffix, sequence in [('', time), ('-duplicate', time+100)]:
                        connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)',
                            (ident+suffix, thread, turn, sequence, time, json.dumps(event)))
                    if ident == 'p2':
                        # Partial cumulative observation and another retained
                        # surface of the same physical attempt are not extra calls.
                        partial = dict(details, status='started', usage=dict(physical_attempt_id=ident))
                        start = dict(kind='item_started', payload=dict(notification,
                                     item=dict(id=ident, code='provider_usage', details=partial)))
                        connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)',
                            ('partial', thread, turn, time+200, time-0.5, json.dumps(start)))
                        mirror = dict(details, observation_id='same-physical-surface')
                        event['payload'] = dict(notification, item=dict(id='same-physical-surface', code='provider_usage', details=mirror))
                        connection.execute('INSERT INTO turn_event VALUES (?,?,?,?,?,?)',
                            ('mirror', thread, turn, time+300, time, json.dumps(event)))
                report = load(connection, [name+'-turn' for name in executions], [], {})
            groups = {(g['connection'], g['execution_cohort']['owner_id']):g for g in report['groups']}
            parent = groups['authority', 'conversation']
            child = groups['authority', 'child-exec']
            self.assertEqual(parent['execution_cohort']['kind'], 'logical_conversation')
            self.assertEqual((parent['all']['observations'], child['all']['observations']), (2, 2))
            self.assertEqual(parent['first_observed']['cache_read'], 80)
            self.assertEqual(parent['subsequent']['cache_read'], 180)
            self.assertEqual(child['first_observed']['cache_read'], 0)
            self.assertEqual(child['subsequent']['cache_read'], 200)
            self.assertAlmostEqual(parent['all']['token_hit_rate'], 100 * 260 / 300)
            self.assertEqual(child['all']['token_hit_rate'], 40)
            provenance = [s for sample in parent['all']['included_observations'] for s in sample['provenance']]
            self.assertEqual({(s['thread_id'], s['turn_id'], s['task_id']) for s in provenance},
                             {('parent-a-exec', 'parent-a-turn', 'parent-a-task'),
                              ('parent-b-exec', 'parent-b-turn', 'parent-b-task')})
            self.assertTrue(all(s['source']=='turn_event' and s['workspace_id']=='workspace'
                                and s['logical_conversation_id']=='conversation' for s in provenance))
            self.assertTrue(all('composer_work_v1' in s['conversation_evidence'] for s in provenance))
            self.assertEqual(any('runtime_snapshot' in s['conversation_evidence'] for s in provenance),
                             storage == 'retained_snapshot')
            self.assertEqual(groups['other-authority', 'conversation']['all']['observations'], 1)
            self.assertEqual(groups['other-authority', 'conversation']['subsequent']['observations'], 0)
            self.assertFalse(groups[None, 'conversation']['connection_known'])
            self.assertIn('continuity unknown', groups[None, 'conversation']['connection_scope'])
            for name in negative:
                group = groups['authority', name+'-exec']
                self.assertEqual(group['execution_cohort']['kind'], 'execution_thread')
                self.assertEqual(group['all']['observations'], 1)
                source, = group['all']['included_observations'][0]['provenance']
                self.assertIsNone(source['logical_conversation_id'])
                self.assertEqual(source['conversation_evidence'], 'unavailable' if name in ['unknown', 'owner-only'] else 'conflicting')

    def test_persisted_auxiliary_authorities_and_legacy_unknown_keep_separate_cohorts(self):
        import json
        import sqlite3
        from prompt_cache_report import load
        with sqlite3.connect(':memory:') as connection:
            connection.execute('CREATE TABLE provider_usage_observation(id TEXT,workspace_id TEXT,owner_id TEXT,operation_kind TEXT,status TEXT,usage_json TEXT,started_at REAL,updated_at REAL)')
            for ident, authority, total, read, time in [
                    ('a1', 'authority-a', 100, 0, 1), ('b1', 'authority-b', 100, 80, 2),
                    ('a2', 'authority-a', 200, 180, 3), ('legacy', None, 100, 50, 4)]:
                usage = dict(provider='openai', api='responses', model='fixture', route='same-endpoint',
                             physical_attempt_id=ident, input_tokens=total, cache_read_input_tokens=read,
                             raw_usage=dict(_pioneer_journal=dict(schema_version=1, connection_id='untrusted-native')))
                if authority:
                    usage['_pioneer_journal'] = dict(schema_version=1, connection_id=authority)
                connection.execute('INSERT INTO provider_usage_observation VALUES (?,?,?,?,?,?,?,?)',
                                   (ident, 'workspace', 'same-owner', 'summary', 'completed', json.dumps(usage), time, time))
            report = load(connection, [], ['same-owner'], {})
        groups = {g['connection']:g for g in report['groups']}
        self.assertEqual(set(groups), {'authority-a', 'authority-b', None})
        self.assertEqual(groups['authority-a']['first_observed']['cache_read'], 0)
        self.assertEqual(groups['authority-a']['subsequent']['cache_read'], 180)
        self.assertEqual(groups['authority-b']['first_observed']['cache_read'], 80)
        self.assertEqual(groups['authority-b']['subsequent']['observations'], 0)
        self.assertFalse(groups[None]['connection_known'])
        self.assertIn('continuity unknown', groups[None]['connection_scope'])
        for group in groups.values():
            self.assertEqual(group['execution_cohort'], dict(kind='operation_owner', workspace_id='workspace', owner_id='same-owner'))
            for sample in group['all']['included_observations']:
                source, = sample['provenance']
                self.assertEqual((source['source'], source['workspace_id'], source['owner_id']),
                                 ('operation_journal', 'workspace', 'same-owner'))
                self.assertEqual(source['connection_id'], group['connection'])
