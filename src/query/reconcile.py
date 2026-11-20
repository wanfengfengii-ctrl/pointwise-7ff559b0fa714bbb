from src.aggregate.daily import build_daily_totals
from src.facts.replay import compare_snapshot_facts
from src.intake.normalize import FieldIntent


class ReconciliationError(ValueError):
    pass


def verify_reconciliation(snapshot, contributions, totals):
    supported = {fact.field_key: fact for fact in snapshot.facts if fact.state is FieldIntent.VALUE}
    supplied = {item.field_key: item for item in contributions}
    if len(supplied) != len(contributions) or supplied.keys() != supported.keys():
        raise ReconciliationError('contributions do not cover exactly the live value facts')
    for key, fact in supported.items():
        item = supplied[key]
        if (item.version, item.day, item.event, item.value, item.unit) != (
                snapshot.version, fact.day, fact.event, fact.value, 'mg'):
            raise ReconciliationError(f'contribution differs from the published fact for {key}')
    if tuple(totals) != build_daily_totals(snapshot.version, contributions):
        raise ReconciliationError('daily totals differ from the published contribution ledger')


def verify_rebuilt_version(snapshot, contributions, totals, rebuilt_snapshot,
                           rebuilt_contributions, rebuilt_totals):
    """Stored ledger must match a rebuild from the received originals and view."""
    verify_reconciliation(snapshot, contributions, totals)
    try:
        compare_snapshot_facts(snapshot, rebuilt_snapshot)
    except ValueError as error:
        raise ReconciliationError(f'cannot reconcile snapshot evidence: {error}') from error
    if tuple(rebuilt_contributions) != tuple(contributions):
        raise ReconciliationError('contributions differ from the rebuilt ledger')
    if tuple(rebuilt_totals) != tuple(totals):
        raise ReconciliationError('daily totals differ from the rebuilt ledger')
