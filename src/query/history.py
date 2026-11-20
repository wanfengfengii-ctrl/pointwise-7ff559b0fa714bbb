from dataclasses import dataclass

from src.aggregate.contributions import Contribution
from src.aggregate.daily import DailyAggregate, build_daily_totals, load_contributions, load_daily_totals
from src.facts.apply import FieldFact
from src.facts.effects import FieldEffect
from src.facts.ledger import rebuild_version, version_received_keys
from src.facts.snapshot import load_snapshot
from src.facts.views import load_view
from src.intake.identity import require_name
from src.intake.normalize import FieldRevision
from src.intake.raw_store import load_raw_events
from src.intake.validation import RawEvent, validate_range
from src.partitions.catalog import resolve_segment
from src.partitions.layout import read_segment
from src.publish.database import read_transaction
from src.publish.receipts import resolve_result_receipt, result_for_version

from .reconcile import ReconciliationError, verify_rebuilt_version


@dataclass(frozen=True)
class EventEvidence:
    original: RawEvent
    revision: FieldRevision
    effect: FieldEffect
    accepted_result: str
    origin_segment: int


@dataclass(frozen=True)
class QualifiedEventEvidence:
    original: RawEvent
    revision: FieldRevision
    effect: FieldEffect | None
    accepted_result: str
    accepted_request: str
    origin_segment: int
    blocked_sources: tuple[str, ...]


@dataclass(frozen=True)
class QueryResult:
    result_id: str
    version: int
    facts: tuple[FieldFact, ...]
    contributions: tuple[Contribution, ...]
    daily: tuple[DailyAggregate, ...]
    evidence: tuple[EventEvidence, ...]


@dataclass(frozen=True)
class QualifiedQueryResult:
    result_id: str
    version: int
    facts: tuple[FieldFact, ...]
    contributions: tuple[Contribution, ...]
    daily: tuple[DailyAggregate, ...]
    evidence: tuple[QualifiedEventEvidence, ...]
    disabled_sources: tuple[str, ...]


def _segments(db):
    cache = {}

    def locate(segment_id):
        if segment_id not in cache:
            segment = read_segment(db, segment_id)
            cache[segment_id] = segment, {item.event_key: item for item in segment.members}
        return cache[segment_id]

    return locate


def _origin_segment(db, locate, key, revision):
    location = resolve_segment(db, key)
    active, live_members = locate(location.active)
    _, original_members = locate(location.original)
    live = live_members.get(key)
    original = original_members.get(key)
    if (active.retired or live is None or original is None or live != original
            or live.raw_ref != key or live.revision != revision):
        raise ReconciliationError(f'incomplete original or active segment evidence for {key}')
    return location.original


def _evidence_v1(db, snapshot, entries):
    locate = _segments(db)
    results = {}

    def accepted_result(accepted_version):
        if accepted_version not in results:
            result_id, _ = result_for_version(db, accepted_version)
            resolve_result_receipt(db, result_id)
            results[accepted_version] = result_id
        return results[accepted_version]

    evidence = []
    for entry, effect in zip(entries, snapshot.effects, strict=True):
        key = entry.raw.identity.key
        origin = _origin_segment(db, locate, key, entry.revision)
        evidence.append(EventEvidence(entry.raw, entry.revision, effect,
                                      accepted_result(entry.accepted_version), origin))
    return tuple(evidence)


def _evidence_qualified(db, snapshot, view):
    locate = _segments(db)
    effects = {effect.event.key: effect for effect in snapshot.effects}
    received = load_raw_events(db, version_received_keys(db, snapshot.version))
    if len(received) != len(view.events):
        raise ReconciliationError('received originals and status view differ in size')
    by_key = {entry.raw.identity.key: entry for entry in received}
    if set(by_key) != {item.key for item in view.events}:
        raise ReconciliationError('received originals and status view cover different events')

    results = {}

    def accepted_result(accepted_version):
        if accepted_version not in results:
            result_id, request_id = result_for_version(db, accepted_version)
            resolve_result_receipt(db, result_id)
            results[accepted_version] = (result_id, request_id)
        return results[accepted_version]

    participants = []
    paused = []
    for item in view.events:
        entry = by_key[item.key]
        origin = _origin_segment(db, locate, item.key, entry.revision)
        result_id, request_id = accepted_result(entry.accepted_version)
        if request_id != item.accepted_request:
            raise ReconciliationError(f'first acceptance request differs for {item.key}')
        if item.paused:
            if item.key in effects:
                raise ReconciliationError(f'paused event {item.key} carries a replay effect')
            paused.append(QualifiedEventEvidence(
                entry.raw, entry.revision, None, result_id,
                item.accepted_request, origin, item.blocked))
        else:
            effect = effects.get(item.key)
            if effect is None:
                raise ReconciliationError(f'participating event {item.key} lacks a replay effect')
            participants.append(QualifiedEventEvidence(
                entry.raw, entry.revision, effect, result_id,
                item.accepted_request, origin, item.blocked))
    participants.sort(key=lambda evidence: evidence.effect.ordinal)
    paused.sort(key=lambda evidence: evidence.original.identity.key)
    if len(participants) != len(snapshot.members):
        raise ReconciliationError('participant evidence and snapshot membership differ')
    if tuple(item.original.identity.key for item in participants) != snapshot.members:
        raise ReconciliationError('participant evidence differs from snapshot membership')
    return tuple(participants) + tuple(paused)


def query_scope(start, end, entities):
    validate_range(start, end)
    if isinstance(entities, (str, bytes)):
        raise ValueError('entities must be a collection of entity identifiers')
    selected = None if entities is None else frozenset(require_name(item, 'entity') for item in entities)

    def includes(entity, day):
        return ((selected is None or entity in selected) and (start is None or day >= start)
                and (end is None or day <= end))

    return includes


def _load_result(db, result_id):
    receipt = resolve_result_receipt(db, result_id)
    snapshot = load_snapshot(db, receipt.version)
    view = load_view(db, snapshot.version)
    rebuilt_snapshot, rebuilt_contributions, rebuilt_totals, blocked = rebuild_version(db, snapshot.version)
    contributions = load_contributions(db, snapshot.version)
    totals = load_daily_totals(db, snapshot.version)
    verify_rebuilt_version(snapshot, contributions, totals,
                           rebuilt_snapshot, rebuilt_contributions, rebuilt_totals)
    rebuilt_blocked = {identity.key: tuple(sources) for identity, sources in blocked}
    return receipt, snapshot, view, contributions, totals, rebuilt_blocked


def read_result(db, result_id, includes):
    receipt, snapshot, view, contributions, totals, rebuilt_blocked = _load_result(db, result_id)
    if view is None:
        entries = load_raw_events(db, snapshot.members)
        evidence = _evidence_v1(db, snapshot, entries)
        selected = tuple(item for item in contributions if includes(item.entity, item.day))
        return QueryResult(result_id, snapshot.version,
                           tuple(fact for fact in snapshot.facts if includes(fact.entity, fact.day)),
                           selected, build_daily_totals(snapshot.version, selected),
                           tuple(item for item in evidence
                                 if includes(item.original.identity.entity, item.original.day)))
    if rebuilt_blocked != {item.key: item.blocked for item in view.events}:
        raise ReconciliationError('blocked-source evidence differs from the rebuilt qualification')
    evidence = _evidence_qualified(db, snapshot, view)
    selected = tuple(item for item in contributions if includes(item.entity, item.day))
    return QualifiedQueryResult(
        result_id, snapshot.version,
        tuple(fact for fact in snapshot.facts if includes(fact.entity, fact.day)),
        selected, build_daily_totals(snapshot.version, selected),
        tuple(item for item in evidence
              if includes(item.original.identity.entity, item.original.day)),
        tuple(view.disabled))


def query_history(db, result_id, *, start=None, end=None, entities=None):
    require_name(result_id, 'result_id')
    includes = query_scope(start, end, entities)
    with read_transaction(db):
        return read_result(db, result_id, includes)
