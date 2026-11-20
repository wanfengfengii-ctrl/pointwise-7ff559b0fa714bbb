import json
import sqlite3

from src.facts.order import ReplayOrderError
from src.intake.identity import identify_event
from src.intake.sources import load_source_status
from src.intake.validation import IntakeError
from src.partitions.catalog import load_catalog
from src.publish.attempts import RequestConflict, load_attempt
from src.publish.coordinator import publish_batch, publish_compaction, publish_source_status
from src.publish.database import initialize_database
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import QualifiedQueryResult, query_history
from src.query.reconcile import ReconciliationError

from tests import DAY, LakeCase, event


def load_sample():
    from pathlib import Path
    path = Path(__file__).resolve().parent.parent / 'examples' / 'source_status.json'
    return json.loads(path.read_text(encoding='utf-8'))


class SourceStatusCase(LakeCase):
    def setUp(self):
        super().setUp()
        self.sample = load_sample()

    def initial(self):
        return publish_batch(self.db, 'base', self.sample['initial'])

    def switch(self, request_id, source, enabled):
        return publish_source_status(self.db, request_id, source, enabled)

    def blocked(self, result):
        return {item.original.identity.event_id: item.blocked_sources for item in result.evidence}


class TestSwitchReceipt(SourceStatusCase):
    def test_switch_receipt_shape_and_idempotency(self):
        base = self.initial()
        catalog = load_catalog(self.db)
        receipt = self.switch('off-p', 'P', False)
        self.assertEqual(receipt.kind, 'source_status')
        self.assertEqual(receipt.events, ())
        self.assertEqual(receipt.replacements, ())
        self.assertEqual(receipt.affected_days, (DAY,))
        self.assertEqual(receipt.catalog_version, catalog.version)
        self.assertNotEqual(receipt.result_id, base.result_id)
        self.assertEqual(find_request_receipt(self.db, 'off-p'), receipt)
        # Same parameters after the state moved elsewhere return the first receipt.
        self.switch('off-q', 'Q', False)
        self.assertEqual(self.switch('off-p', 'P', False), receipt)
        self.assertTrue(load_source_status(self.db)['P'] is False)
        # No new catalog segment for a status switch.
        self.assertEqual(load_catalog(self.db), catalog)

    def test_unknown_source_parameters_and_conflicts(self):
        self.initial()
        with self.assertRaisesRegex(ValueError, 'unknown source'):
            self.switch('off-x', 'missing', False)
        self.assertIsNone(find_request_receipt(self.db, 'off-x'))
        with self.assertRaisesRegex(ValueError, 'boolean'):
            publish_source_status(self.db, 'off-p', 'P', 0)
        self.assertIsNone(find_request_receipt(self.db, 'off-p'))
        with self.assertRaisesRegex(ValueError, 'already enabled'):
            self.switch('noop', 'P', True)
        self.assertIsNone(find_request_receipt(self.db, 'noop'))
        self.switch('off-p', 'P', False)
        with self.assertRaisesRegex(ValueError, 'already disabled'):
            self.switch('noop2', 'P', False)
        with self.assertRaises(RequestConflict):
            publish_batch(self.db, 'off-p', self.sample['initial'])
        with self.assertRaises(RequestConflict):
            publish_source_status(self.db, 'off-p', 'P', True)

    def test_switch_before_any_data_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'requires a published data snapshot'):
            self.switch('early', 'P', False)
        self.assertIsNone(find_request_receipt(self.db, 'early'))

    def test_bad_switch_parameters_bind_nothing(self):
        self.initial()
        for request_id, source, enabled, message in (
                ('bad-name', 'P', 'yes', 'boolean'),
                ('blank-source', '  ', False, 'source'),
                ('', 'P', True, 'request_id'),
        ):
            with self.subTest(request_id=request_id):
                with self.assertRaisesRegex(ValueError, message):
                    publish_source_status(self.db, request_id, source, enabled)
                self.assertIsNone(find_request_receipt(self.db, request_id))
                if request_id:
                    self.assertIsNone(load_attempt(self.db, request_id))
        # An unknown source is a business rejection: the binding is kept with a
        # failure diagnostic, and the state is untouched.
        status_before = load_source_status(self.db)
        with self.assertRaisesRegex(ValueError, 'unknown source'):
            self.switch('unknown', 'no-such', True)
        self.assertIsNone(find_request_receipt(self.db, 'unknown'))
        self.assertEqual(load_attempt(self.db, 'unknown').status, 'failed')
        self.assertEqual(load_source_status(self.db), status_before)

    def test_switch_failure_is_atomic_and_retryable(self):
        self.initial()
        self.switch('off-p', 'P', False)
        before = query_current(self.db)
        self.db.execute('''CREATE TRIGGER reject_daily BEFORE INSERT ON daily_totals
                           BEGIN SELECT RAISE(ABORT, 'daily table unavailable'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'daily table unavailable'):
            self.switch('off-r', 'R', False)
        reader = self.connect()
        self.assertEqual(load_source_status(reader)['R'], True)
        self.assertIsNone(find_request_receipt(reader, 'off-r'))
        self.assertEqual(query_current(reader), before)
        self.assertEqual(load_attempt(reader, 'off-r').status, 'failed')
        reader.execute('DROP TRIGGER reject_daily')
        retried = publish_source_status(reader, 'off-r', 'R', False)
        self.assertEqual([(item.total_mg, item.count) for item in query_current(reader).daily],
                         [('5', 1)])
        self.assertEqual(retried.affected_days, ())
        self.assertEqual(load_source_status(reader)['R'], False)


    def test_restore_failure_keeps_old_ledger_and_retries(self):
        self.initial()
        self.switch('off-p', 'P', False)
        publish_batch(self.db, 'late', [self.sample['initial'][0], self.sample['late']])
        self.switch('on-p', 'P', True)
        self.switch('off-p-2', 'P', False)
        self.switch('off-q', 'Q', False)
        self.switch('on-q', 'Q', True)
        paused = query_current(self.db)
        self.assertEqual([(item.total_mg, item.count) for item in paused.daily], [('5', 1)])
        self.db.execute('''CREATE TRIGGER reject_daily BEFORE INSERT ON daily_totals
                           BEGIN SELECT RAISE(ABORT, 'daily table unavailable'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'daily table unavailable'):
            self.switch('on-p-again', 'P', True)
        reader = self.connect()
        self.assertEqual(load_source_status(reader)['P'], False)
        self.assertEqual(query_current(reader), paused)
        self.assertIsNone(find_request_receipt(reader, 'on-p-again'))
        reader.execute('DROP TRIGGER reject_daily')
        retried = publish_source_status(reader, 'on-p-again', 'P', True)
        self.assertEqual([(item.total_mg, item.count) for item in query_current(reader).daily],
                         [('27', 4)])
        self.assertEqual(retried.affected_days, (DAY,))
        self.assertEqual(query_history(reader, paused.result_id), paused)


class TestBlocking(SourceStatusCase):
    def test_disablement_follows_explicit_predecessors(self):
        self.initial()
        self.switch('off-p', 'P', False)
        result = query_current(self.db)
        self.assertIsInstance(result, QualifiedQueryResult)
        self.assertEqual([(item.total_mg, item.count) for item in result.daily], [('5', 1)])
        self.assertEqual(result.disabled_sources, ('P',))
        self.assertEqual(self.blocked(result),
                         {'a': ('P',), 'b': (), 'c': ('P',), 'd': ('P',)})
        self.switch('off-q', 'Q', False)
        result = query_current(self.db)
        self.assertEqual(result.daily, ())
        self.assertEqual(self.blocked(result),
                         {'a': ('P',), 'b': ('Q',), 'c': ('P', 'Q'), 'd': ('P', 'Q')})
        self.assertEqual([item.original.identity.event_id for item in result.evidence],
                         ['a', 'b', 'c', 'd'])
        for item in result.evidence:
            self.assertIsNone(item.effect)

    def test_sequence_only_neighbour_does_not_propagate(self):
        self.initial()
        self.switch('off-p', 'P', False)
        publish_batch(self.db, 'manual', [self.sample['sequence_only']])
        result = query_current(self.db)
        self.assertEqual([(item.total_mg, item.count) for item in result.daily], [('8', 2)])
        blocked = self.blocked(result)
        self.assertEqual(blocked['manual-a'], ())
        self.assertEqual(blocked['a'], ('P',))
        self.assertEqual(blocked['c'], ('P',))
        participants = [item.original.identity.event_id for item in result.evidence if item.effect]
        self.assertEqual(participants, ['b', 'manual-a'])

    def test_no_participants_leaves_empty_ledger(self):
        self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        self.switch('off-r', 'R', False)
        result = query_current(self.db)
        self.assertEqual((result.facts, result.contributions, result.daily), ((), (), ()))
        self.assertEqual(len(result.evidence), 4)
        self.assertTrue(all(item.blocked_sources for item in result.evidence))

    def test_late_arrival_while_paused_is_first_accepted(self):
        base = self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        late = publish_batch(self.db, 'late', [self.sample['initial'][0], self.sample['late']])
        provenance = {item.event.event_id: (item.request_id, item.result_id) for item in late.events}
        self.assertEqual(provenance['a'], ('base', base.result_id))
        self.assertEqual(provenance['late-a'], ('late', late.result_id))
        result = query_current(self.db)
        self.assertEqual(result.daily, ())
        evidence = {item.original.identity.event_id: item for item in result.evidence}
        self.assertEqual(evidence['late-a'].blocked_sources, ('P',))
        self.assertEqual((evidence['late-a'].accepted_request, evidence['late-a'].accepted_result),
                         ('late', late.result_id))
        self.assertEqual(evidence['a'].accepted_request, 'base')
        self.switch('on-p', 'P', True)
        self.assertEqual([item.total_mg for item in query_current(self.db).daily], ['13'])
        self.switch('on-q', 'Q', True)
        final = query_current(self.db)
        self.assertEqual([(item.total_mg, item.count) for item in final.daily], [('27', 4)])
        self.assertEqual([item.original.identity.event_id for item in final.evidence],
                         ['a', 'b', 'c', 'd', 'late-a'])
        self.assertEqual(final.disabled_sources, ())

    def test_validation_ignores_pause_state(self):
        self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        before = query_current(self.db)
        counts = self.counts()
        for name in ('conflict', 'missing'):
            with self.subTest(name=name):
                with self.assertRaises(ReplayOrderError):
                    publish_batch(self.db, 'try-' + name, [self.sample[name]])
                self.assertIsNone(find_request_receipt(self.db, 'try-' + name))
                self.assertEqual(self.counts(), counts)
        self.assertEqual(query_current(self.db), before)

    def test_cycle_across_paused_sources_is_rejected(self):
        self.initial()
        self.switch('off-p', 'P', False)
        counts = self.counts()
        left = event('cycle-left', 7, 1, field='X', source='P')
        right = event('cycle-right', 8, 2, field='Y', source='P',
                      requires=[identify_event(left).parts])
        left = dict(left, requires=[['cycle-right', 'E', 'Y', 'P']])
        with self.assertRaises(ReplayOrderError):
            publish_batch(self.db, 'cycle', [left, right])
        self.assertIsNone(find_request_receipt(self.db, 'cycle'))
        self.assertEqual(self.counts(), counts)

    def test_explicit_predecessor_identity_is_exact(self):
        """A later same-field identity cannot stand in for an absent predecessor."""
        original = event('a', 1, 12, source='P')
        later = event('a2', 2, 9, source='P')
        publish_batch(self.db, 'seed', [original])
        publish_batch(self.db, 'later', [later])
        publish_source_status(self.db, 'off-p', 'P', False)
        # E/A already has a later write 'a2', but the declared predecessor
        # four-tuple names 'a3', which was never accepted: that is missing.
        wrong = event('dep', 3, 4, field='B', source='Q',
                      requires=[['a3', 'E', 'A', 'P']])
        counts = self.counts()
        with self.assertRaisesRegex(ReplayOrderError, 'missing predecessor'):
            publish_batch(self.db, 'wrong-ref', [wrong])
        self.assertIsNone(find_request_receipt(self.db, 'wrong-ref'))
        self.assertEqual(self.counts(), counts)
        right = event('dep', 3, 4, field='B', source='Q',
                      requires=[identify_event(original).parts])
        publish_batch(self.db, 'right-ref', [right])
        result = query_current(self.db)
        evidence = {item.original.identity.event_id: item for item in result.evidence}
        self.assertEqual(evidence['dep'].blocked_sources, ('P',))
        self.assertIsNone(evidence['dep'].effect)
        self.assertEqual(query_current(self.db, entities=['Z']).facts, ())


class TestHistoryAndCompaction(SourceStatusCase):
    def test_old_v1_results_keep_old_shape(self):
        base = self.initial()
        old = query_history(self.db, base.result_id)
        self.assertFalse(isinstance(old, QualifiedQueryResult))
        self.assertFalse(hasattr(old, 'disabled_sources'))
        self.switch('off-p', 'P', False)
        self.assertEqual(query_history(self.db, base.result_id), old)
        self.assertEqual([(item.total_mg, item.count) for item in old.daily], [('26', 4)])

    def test_versions_are_frozen(self):
        base = self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        late = publish_batch(self.db, 'late', [self.sample['initial'][0], self.sample['late']])
        paused = query_history(self.db, late.result_id)
        self.switch('on-p', 'P', True)
        self.switch('on-q', 'Q', True)
        self.assertEqual(query_history(self.db, late.result_id), paused)
        self.assertEqual(paused.daily, ())
        self.assertEqual(paused.disabled_sources, ('P', 'Q'))
        self.assertEqual({item.original.identity.event_id: item.blocked_sources
                          for item in paused.evidence}['late-a'], ('P',))
        self.assertEqual([item.total_mg for item in query_history(self.db, base.result_id).daily], ['26'])

    def test_scoped_query_keeps_full_version_validation(self):
        self.initial()
        self.switch('off-p', 'P', False)
        scoped = query_current(self.db, start=DAY, end=DAY, entities=['E'])
        self.assertEqual([(item.total_mg, item.count) for item in scoped.daily], [('5', 1)])
        self.assertEqual(len(scoped.evidence), 4)
        self.assertEqual(query_current(self.db, entities=['X']).evidence, ())

    def test_tampered_qualification_evidence_is_rejected(self):
        self.initial()
        switched = self.switch('off-p', 'P', False)
        good = query_current(self.db)
        self.db.execute('''UPDATE source_version_events SET blocked_json = '["Q"]'
                           WHERE version = ? AND event_key LIKE '%"a"%' ''', (switched.version,))
        with self.assertRaisesRegex(ReconciliationError, 'blocked-source evidence'):
            query_current(self.db)
        self.db.execute('''UPDATE source_version_events SET blocked_json = '["P"]'
                           WHERE version = ? AND event_key LIKE '%"a"%' ''', (switched.version,))
        self.assertEqual(query_current(self.db), good)
        self.db.execute('DELETE FROM source_version_events WHERE version = ? AND event_key LIKE ?',
                        (switched.version, '%"c"%'))
        with self.assertRaises(ReconciliationError):
            query_current(self.db)
        # The earlier V1 result never relied on the status view.
        self.assertEqual([(item.total_mg, item.count)
                          for item in query_history(self.db, 'result-1').daily], [('26', 4)])

    def test_missing_paused_original_is_rejected(self):
        self.initial()
        self.switch('off-p', 'P', False)
        self.db.execute('DELETE FROM raw_events WHERE event_key LIKE ?', ('%"a"%E%A%P%',))
        with self.assertRaisesRegex(IntakeError, 'missing original event'):
            query_current(self.db)

    def test_compaction_preserves_paused_originals_and_provenance(self):
        self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        late = publish_batch(self.db, 'late', [self.sample['initial'][0], self.sample['late']])
        before = query_current(self.db)
        segments_before = {item.original.identity.event_id: item.origin_segment
                           for item in before.evidence}
        catalog = load_catalog(self.db)
        maintenance = publish_compaction(self.db, 'pack', DAY, DAY, catalog.version)
        self.assertEqual(maintenance.result_id, late.result_id)
        self.assertTrue(maintenance.replacements)
        after = query_current(self.db)
        self.assertEqual(after, before)
        self.assertEqual({item.original.identity.event_id: item.origin_segment
                          for item in after.evidence}, segments_before)
        self.assertEqual(self.blocked(after)['late-a'], ('P',))
        self.db.close()
        reopened = self.connect()
        self.assertEqual(query_current(reopened), before)
        self.assertEqual(query_history(reopened, late.result_id), before)


class TestZeroAndResubmission(SourceStatusCase):
    def test_v1_sample_baseline_still_holds(self):
        from examples import source_status_baseline
        source_status_baseline.main()

    def test_original_cli_demo_still_runs(self):
        from src.publish.cli import main
        from pathlib import Path
        self.assertEqual(main(['demo', str(Path(__file__).resolve().parent.parent
                                          / 'examples' / 'day.json')]), 0)

    def test_upgrading_an_old_database(self):
        base = self.initial()
        old = query_history(self.db, base.result_id)
        self.db.close()
        with sqlite3.connect(self.path) as raw:
            for table in ('source_version_events', 'source_versions', 'received_events', 'source_status'):
                raw.execute(f'DROP TABLE {table}')
        reopened = self.connect()
        initialize_database(reopened)
        initialize_database(reopened)
        self.assertEqual(query_history(reopened, base.result_id), old)
        self.assertEqual(dict(reopened.execute('SELECT source, enabled FROM source_status ORDER BY source')),
                         {'P': 1, 'Q': 1, 'R': 1})
        # A V1 publication after the upgrade keeps the old result shape.
        publish_batch(reopened, 'next', [self.sample['sequence_only']])
        self.assertNotIsInstance(query_current(reopened), QualifiedQueryResult)
        switched = publish_source_status(reopened, 'off-p', 'P', False)
        self.assertEqual(switched.kind, 'source_status')
        self.assertEqual([(item.total_mg, item.count) for item in query_current(reopened).daily],
                         [('8', 2)])

    def test_empty_ledger_round_trips(self):
        self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        empty = query_current(self.db)
        self.assertEqual(empty.daily, ())
        publish_compaction(self.db, 'pack', DAY, DAY, load_catalog(self.db).version)
        self.assertEqual(query_current(self.db), empty)

    def test_resubmitting_old_and_late_events_returns_first_receipts(self):
        base = self.initial()
        self.switch('off-p', 'P', False)
        self.switch('off-q', 'Q', False)
        late = publish_batch(self.db, 'late', [self.sample['initial'][0], self.sample['late']])
        self.switch('on-p', 'P', True)
        self.switch('on-q', 'Q', True)
        counts = self.counts()
        again = publish_batch(self.db, 'again', [self.sample['initial'][0], self.sample['late']])
        self.assertIsNone(again.result_id)
        self.assertEqual({item.event.event_id: item.result_id for item in again.events},
                         {'a': base.result_id, 'late-a': late.result_id})
        self.assertEqual(find_request_receipt(self.db, 'again'), again)
        self.assertEqual(self.counts()[:2], (5, 6))
        self.assertEqual(self.counts()[2:6], counts[2:6])
