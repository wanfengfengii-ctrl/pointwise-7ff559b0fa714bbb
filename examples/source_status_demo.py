"""Demo of source suspension, first acceptance while paused, compaction and staged recovery.

Runs the whole source-status scenario on a temporary database and prints one
JSON summary of the actual ledgers, provenance and first receipts.  Exit code
is non-zero when any expected business fact fails to hold.
"""

import json
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from src.partitions.catalog import load_catalog
from src.publish.coordinator import publish_batch, publish_compaction, publish_source_status
from src.publish.database import initialize_database, open_database
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history


def _sample():
    path = Path(__file__).with_name('source_status.json')
    return json.loads(path.read_text(encoding='utf-8'))


def _daily(result):
    return [[item.total_mg, item.count] for item in result.daily]


def _evidence(result):
    return [dict(event=item.original.identity.event_id, request=item.accepted_request,
                 accepted=item.accepted_result, blocked=list(item.blocked_sources),
                 effect=None if item.effect is None else item.effect.ordinal,
                 segment=item.origin_segment) for item in result.evidence]


def _expect(condition, message):
    if not condition:
        raise AssertionError(message)


def _run(db, sample):
    day = sample['day']
    initial = sample['initial']
    late_records = [initial[0], sample['late']]
    report = {}

    base = publish_batch(db, 'base', initial)
    report['initial'] = _daily(query_current(db))
    _expect(report['initial'] == [['26', 4]], 'initial ledger is not 26 mg over 4 contributions')

    disabled_p = publish_source_status(db, 'disable-p', 'P', False)
    _expect(disabled_p.kind == 'source_status' and disabled_p.events == () and disabled_p.replacements == (),
            'switch receipt is not an empty source_status receipt')
    report['after_disable_p'] = dict(daily=_daily(query_current(db)),
                                     disabled=list(query_current(db).disabled_sources))
    _expect(report['after_disable_p']['daily'] == [['5', 1]], 'disabling P did not leave only 5 mg')

    disabled_q = publish_source_status(db, 'disable-q', 'Q', False)
    report['after_disable_q'] = dict(daily=_daily(query_current(db)),
                                     disabled=list(query_current(db).disabled_sources))
    _expect(report['after_disable_q']['daily'] == [], 'disabling both sources did not empty the day')

    late = publish_batch(db, 'late', late_records)
    provenance = {item.event.event_id: (item.request_id, item.result_id) for item in late.events}
    report['late_receipts'] = provenance
    _expect(provenance['a'] == ('base', base.result_id), 'old event lost its first acceptance')
    _expect(provenance['late-a'] == ('late', late.result_id), 'new event was not first accepted while paused')
    late_view = query_current(db)
    paused = {item.original.identity.event_id: list(item.blocked_sources) for item in late_view.evidence}
    report['paused_day'] = dict(daily=_daily(late_view), blocked=paused)
    _expect(paused['late-a'] == ['P'], 'late-a is not blocked by P alone')
    _expect(paused['c'] == ['P', 'Q'], 'c is not blocked by both sources')

    catalog = load_catalog(db)
    maintenance = publish_compaction(db, 'pack', day, day, catalog.version)
    _expect(maintenance.result_id == late.result_id, 'compaction changed the logical result')
    packed = query_current(db)
    packed_evidence = {item.original.identity.event_id: item for item in packed.evidence}
    _expect(packed == late_view, 'reopened compacted read differs from the paused ledger')
    _expect(packed_evidence['late-a'].accepted_request == 'late', 'compaction lost late-a first acceptance')
    _expect(packed_evidence['late-a'].origin_segment == 2, 'compaction moved late-a origin segment')
    report['after_compact'] = dict(result=packed.result_id, daily=_daily(packed), evidence=_evidence(packed))

    publish_source_status(db, 'enable-p', 'P', True)
    report['after_enable_p'] = dict(daily=_daily(query_current(db)),
                                    disabled=list(query_current(db).disabled_sources))
    _expect(report['after_enable_p']['daily'] == [['13', 1]], 'restoring P did not replay A at late-a')
    publish_source_status(db, 'enable-q', 'Q', True)
    final = query_current(db)
    report['after_enable_q'] = dict(daily=_daily(final), disabled=list(final.disabled_sources),
                                    evidence=_evidence(final))
    _expect(report['after_enable_q']['daily'] == [['27', 4]], 'restoring Q did not reach 27 mg over 4')
    _expect(final.evidence[0].original.identity.event_id == 'a'
            and final.evidence[-1].original.identity.event_id == 'late-a',
            'final replay order is not a then late-a')

    report['base_reread'] = _daily(query_history(db, base.result_id))
    _expect(report['base_reread'] == [['26', 4]], 're-reading the first result gained late-a')
    report['late_reread'] = dict(daily=_daily(query_history(db, late.result_id)),
                                 disabled=list(query_history(db, late.result_id).disabled_sources))
    _expect(report['late_reread']['daily'] == [], 'the paused-version result changed after recovery')

    for name in ('conflict', 'missing'):
        try:
            publish_batch(db, 'retry-' + name, [sample[name]])
        except ValueError:
            _expect(find_request_receipt(db, 'retry-' + name) is None,
                    f'rejected {name} left a success receipt')
        else:
            raise AssertionError(f'accepted invalid {name} fixture')
    report['rejected'] = ['conflict', 'missing']
    _expect(query_current(db) == final, 'a rejected request changed the current ledger')
    return report


def demo():
    sample = _sample()
    with TemporaryDirectory(prefix='mass-source-status-') as directory:
        path = Path(directory) / 'observations.sqlite'
        with closing(open_database(path)) as db:
            initialize_database(db)
            report = _run(db, sample)
        with closing(open_database(path)) as reopened:
            initialize_database(reopened)
            report['reopened'] = dict(daily=_daily(query_current(reopened)),
                                      result=query_current(reopened).result_id)
            _expect(report['reopened']['daily'] == [['27', 4]], 'reopening lost the recovered ledger')
    scenario = dict(
        case_id='source_status.suspend_receive_compact_restore',
        input=sample,
        expected=dict(initial=[['26', 4]], disable_p=[['5', 1]], disable_q=[],
                      late=[], restored_p=[['13', 1]], restored_q=[['27', 4]]),
        actual=report,
        code_paths=['src/publish/coordinator.py', 'src/facts/eligibility.py',
                    'src/facts/ledger.py', 'src/query/history.py', 'src/partitions/compact.py'],
        oracle='停用沿显式前置传递；补送照常首收；压实保全原件与出处；分次恢复逐日回账。')
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, sort_keys=True, ensure_ascii=False))
    return report


def main():
    try:
        demo()
    except AssertionError as error:
        print(f'AssertionError: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
