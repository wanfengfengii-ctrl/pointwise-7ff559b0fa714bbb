import sqlite3

from src.aggregate.contributions import affected_contribution_dates, derive_contributions
from src.aggregate.daily import (build_daily_totals, load_contributions, load_daily_totals,
                                 save_daily_totals)
from src.facts.ledger import build_version, rebuild_version, store_received, version_received_keys
from src.facts.replay import compare_snapshot_facts, replay_events
from src.facts.snapshot import load_snapshot, save_snapshot
from src.facts.views import SourceView, ViewEvent, load_view, save_view
from src.intake.accept import accept_batch, unique_events
from src.intake.identity import require_name
from src.intake.raw_store import load_raw_events
from src.intake.sources import disabled_sources, load_source_status, set_source_enabled
from src.intake.validation import validate_batch, validate_day, validate_range
from src.partitions.catalog import (load_catalog, publish_partition_update, publish_replacement,
                                    read_active_segments)
from src.partitions.compact import plan_compaction, stage_replacement
from src.partitions.layout import stage_partition_segments
from src.partitions.retire import retire_replaced_segments
from src.query.reconcile import verify_reconciliation

from .attempts import begin_attempt, bind_request_parameters, complete_attempt, record_failure
from .receipts import (EventReceipt, PublishReceipt, find_request_receipt, load_current_result,
                       resolve_result_receipt, result_for_version, save_success_receipt)
from .rollback import publication_transaction


def _run_publication(db, request_id, kind, parameters, action):
    # Keep the binding after a failed publication so a retry has the same meaning.
    with publication_transaction(db):
        bind_request_parameters(db, request_id, kind, parameters)
        existing = find_request_receipt(db, request_id)
    if existing is not None:
        return existing
    try:
        with publication_transaction(db):
            existing = find_request_receipt(db, request_id)
            if existing is not None:
                return existing
            begin_attempt(db, request_id)
            receipt, make_current = action()
            save_success_receipt(db, receipt, make_current=make_current)
            complete_attempt(db, request_id)
            return receipt
    except (ValueError, LookupError, sqlite3.Error) as error:
        try:
            with publication_transaction(db):
                if find_request_receipt(db, request_id) is None:
                    record_failure(db, request_id, f'{type(error).__name__}: {error}')
        except sqlite3.Error as diagnostic_error:
            error.add_note(f'could not persist attempt diagnostics: {diagnostic_error}')
        raise


def _current_snapshot(db):
    result_id = load_current_result(db)
    if result_id is None:
        return None, None
    receipt = resolve_result_receipt(db, result_id)
    return result_id, load_snapshot(db, receipt.version)


def _accepted_requests(db, entries, version, request_id):
    """First-acceptance request per event; the current batch owns new events."""
    by_version = {}
    requests = {}
    for entry in entries:
        key = entry.raw.identity.key
        accepted = entry.accepted_version
        if accepted == version:
            requests[key] = request_id
            continue
        if accepted > version:
            raise ValueError(f'original {key} claims a future acceptance version')
        if accepted not in by_version:
            by_version[accepted] = result_for_version(db, accepted)[1]
        requests[key] = by_version[accepted]
    return requests


def _first_acceptance(db, entry):
    result_id, request_id = result_for_version(db, entry.accepted_version)
    return result_id, request_id


def _view_for_version(version, entries, blocked, disabled, requests):
    blocked_by_identity = dict(blocked)
    events = tuple(ViewEvent(entry.raw.identity.key, requests[entry.raw.identity.key],
                             blocked_by_identity.get(entry.raw.identity, ()))
                   for entry in entries)
    return SourceView(version, tuple(disabled), events)


def _blocked_map(blocked):
    return {identity.key: tuple(sources) for identity, sources in blocked}


def _verify_stored_version(db, version, expect_disabled=None, expect_requests=None):
    stored = load_snapshot(db, version)
    rebuilt, contributions, totals, blocked = rebuild_version(db, version)
    compare_snapshot_facts(stored, rebuilt)
    if load_contributions(db, version) != contributions:
        raise ValueError(f'rebuilt contributions differ from version {version}')
    if load_daily_totals(db, version) != totals:
        raise ValueError(f'rebuilt daily ledger differs from version {version}')
    view = load_view(db, version)
    if view is None:
        if blocked:
            raise ValueError(f'version {version} has blocking evidence without a status view')
        return stored
    if expect_disabled is not None and view.disabled != tuple(expect_disabled):
        raise ValueError(f'disabled sources differ from the requested switch for version {version}')
    received = version_received_keys(db, version)
    keys = tuple(item.key for item in view.events)
    if tuple(sorted(keys)) != tuple(sorted(received)) or len(set(keys)) != len(keys):
        raise ValueError(f'status view does not cover the received originals of version {version}')
    if _blocked_map(blocked) != {item.key: item.blocked for item in view.events}:
        raise ValueError(f'blocked-source evidence differs from version {version}')
    for item in view.events:
        if expect_requests is not None:
            expected = expect_requests[item.key]
        else:
            entry = load_raw_events(db, (item.key,))[0]
            expected = _first_acceptance(db, entry)[1]
        if item.accepted_request != expected:
            raise ValueError(f'first acceptance request differs for {item.key}')
    return stored


def _event_receipts(db, entries):
    by_version = {}
    receipts = []
    for entry in entries:
        if entry.accepted_version not in by_version:
            by_version[entry.accepted_version] = result_for_version(db, entry.accepted_version)
        result_id, request_id = by_version[entry.accepted_version]
        receipts.append(EventReceipt(entry.raw.identity, result_id, request_id))
    return receipts


def _v1_prior_members(db, previous, catalog):
    segments = read_active_segments(db, catalog)
    members = {item.event_key: item for segment in segments.values() for item in segment.members}
    expected = () if previous is None else previous.members
    if set(members) != set(expected):
        raise ValueError('catalog membership differs from the current snapshot')
    entries = load_raw_events(db, expected)
    for entry in entries:
        key = entry.raw.identity.key
        member = members[key]
        if member.raw_ref != key or member.revision != entry.revision:
            raise ValueError(f'active segment differs from the original event {key}')
    return members


def _publish_batch(db, request_id, events):
    _, previous = _current_snapshot(db)
    catalog = load_catalog(db)
    version = 1 if previous is None else previous.version + 1
    accepted = accept_batch(db, events, version)
    repeated = _event_receipts(db, accepted.repeated)
    if not accepted.added:
        originals = {item.result_id for item in repeated}
        result_id = next(iter(originals)) if len(originals) == 1 else None
        original_version = resolve_result_receipt(db, result_id).version if result_id is not None else None
        return PublishReceipt(request_id, 'batch', result_id, original_version, tuple(repeated)), False

    view = None if previous is None else load_view(db, previous.version)
    if view is None:
        return _publish_batch_v1(db, request_id, previous, catalog, version, accepted, repeated)
    return _publish_batch_qualified(db, request_id, previous, catalog, version,
                                    accepted, repeated, view.disabled)


def _publish_batch_v1(db, request_id, previous, catalog, version, accepted, repeated):
    prior_members = _v1_prior_members(db, previous, catalog)
    revisions = tuple(entry.revision for entry in accepted.added)
    changed = {item.identity.key for item in revisions}
    all_revisions = [member.revision for member in prior_members.values()] + list(revisions)
    replayed = replay_events(all_revisions, version,
                             expected_keys=set(prior_members) | changed, changed_keys=changed)
    snapshot = replayed.snapshot
    contributions = derive_contributions(snapshot)
    prior_contributions = () if previous is None else load_contributions(db, previous.version)
    affected_days = affected_contribution_dates(prior_contributions, contributions)
    totals = build_daily_totals(version, contributions)
    verify_reconciliation(snapshot, contributions, totals)

    staged = stage_partition_segments(db, revisions)
    save_snapshot(db, snapshot)
    save_daily_totals(db, totals, contributions)
    store_received(db, version, snapshot.members)
    verify_reconciliation(snapshot, load_contributions(db, version), load_daily_totals(db, version))
    catalog_version = publish_partition_update(db, catalog.version, staged)
    result_id = f'result-{version}'
    accepted_receipts = repeated + [EventReceipt(item.identity, result_id, request_id) for item in revisions]
    receipts = tuple(sorted(accepted_receipts, key=lambda item: item.event.key))
    return PublishReceipt(request_id, 'batch', result_id, version, receipts,
                          catalog_version, affected_days=affected_days), True


def _verify_active_covers(db, keys):
    """Fail early when the physical catalog lost a received original."""
    segments = read_active_segments(db, load_catalog(db))
    members = {item.event_key: item for segment in segments.values() for item in segment.members}
    if set(members) != set(keys):
        raise ValueError('catalog membership differs from the received originals')
    return members


def _publish_batch_qualified(db, request_id, previous, catalog, version,
                             accepted, repeated, disabled):
    status = load_source_status(db)
    if disabled_sources(status) != tuple(disabled):
        raise ValueError('source status changed since the previous version')
    prior_keys = version_received_keys(db, previous.version)
    _verify_active_covers(db, prior_keys)
    entries = load_raw_events(db)
    added_keys = {entry.raw.identity.key for entry in accepted.added}
    if not set(prior_keys) | added_keys == {entry.raw.identity.key for entry in entries}:
        raise ValueError('accepted originals do not extend the previous received set')
    snapshot, contributions, totals, blocked = build_version(entries, version, disabled)
    prior_contributions = load_contributions(db, previous.version)
    affected_days = affected_contribution_dates(prior_contributions, contributions)
    verify_reconciliation(snapshot, contributions, totals)

    staged = stage_partition_segments(db, [entry.revision for entry in accepted.added])
    save_snapshot(db, snapshot)
    save_daily_totals(db, totals, contributions)
    store_received(db, version, [entry.raw.identity.key for entry in entries])
    requests = _accepted_requests(db, entries, version, request_id)
    save_view(db, _view_for_version(version, entries, blocked, disabled, requests))
    _verify_stored_version(db, version, expect_disabled=disabled, expect_requests=requests)
    catalog_version = publish_partition_update(db, catalog.version, staged)
    result_id = f'result-{version}'
    accepted_receipts = repeated + [EventReceipt(item.raw.identity, result_id, request_id)
                                    for item in accepted.added]
    receipts = tuple(sorted(accepted_receipts, key=lambda item: item.event.key))
    return PublishReceipt(request_id, 'batch', result_id, version, receipts,
                          catalog_version, affected_days=affected_days), True


def publish_batch(db, request_id, records):
    events = unique_events(validate_batch(records))
    parameters = [event.to_record() for event in events]
    return _run_publication(db, request_id, 'batch', parameters,
                            lambda: _publish_batch(db, request_id, events))


def _publish_source_status(db, request_id, source, enabled):
    result_id, previous = _current_snapshot(db)
    if previous is None:
        raise ValueError('a source switch requires a published data snapshot')
    status = load_source_status(db)
    if source not in status:
        raise ValueError(f'unknown source {source!r}')
    if status[source] is enabled:
        raise ValueError(f'source {source!r} is already {"enabled" if enabled else "disabled"}')

    updated = dict(status)
    updated[source] = enabled
    disabled = disabled_sources(updated)
    version = previous.version + 1
    catalog = load_catalog(db)
    _verify_active_covers(db, version_received_keys(db, previous.version))
    entries = load_raw_events(db)
    snapshot, contributions, totals, blocked = build_version(entries, version, disabled)
    prior_contributions = load_contributions(db, previous.version)
    affected_days = affected_contribution_dates(prior_contributions, contributions)
    verify_reconciliation(snapshot, contributions, totals)

    save_snapshot(db, snapshot)
    save_daily_totals(db, totals, contributions)
    store_received(db, version, [entry.raw.identity.key for entry in entries])
    requests = _accepted_requests(db, entries, version, request_id)
    save_view(db, _view_for_version(version, entries, blocked, disabled, requests))
    _verify_stored_version(db, version, expect_disabled=disabled, expect_requests=requests)
    set_source_enabled(db, source, enabled)
    result_id = f'result-{version}'
    return PublishReceipt(request_id, 'source_status', result_id, version,
                          catalog_version=catalog.version,
                          affected_days=affected_days), True


def publish_source_status(db, request_id, source, enabled):
    require_name(request_id, 'request_id')
    require_name(source, 'source')
    if type(enabled) is not bool:
        raise ValueError('enabled must be a Python boolean')
    return _run_publication(db, request_id, 'source_status', [source, enabled],
                            lambda: _publish_source_status(db, request_id, source, enabled))


def _active_received_entries(db, current):
    """Verify the active segments carry every received original of the version."""
    segments = read_active_segments(db, load_catalog(db))
    members = {item.event_key: item for segment in segments.values() for item in segment.members}
    expected_keys = set(version_received_keys(db, current.version))
    if set(members) != expected_keys:
        raise ValueError('catalog membership differs from the received originals')
    entries = load_raw_events(db, tuple(sorted(members)))
    by_key = {}
    for entry in entries:
        key = entry.raw.identity.key
        member = members[key]
        if member.raw_ref != key or member.revision != entry.revision:
            raise ValueError(f'active segment differs from the original event {key}')
        by_key[key] = entry
    return segments, members, by_key


def _publish_compaction(db, request_id, start, end, source_version):
    plan = plan_compaction(db, request_id, start, end, source_version)
    result_id, current = _current_snapshot(db)
    if current is None:
        raise ValueError('compaction requires a published data snapshot')
    view = load_view(db, current.version)
    active, members, by_key = _active_received_entries(db, current)
    replacements = stage_replacement(db, plan)
    for replacement in replacements:
        for source_id in replacement.source_ids:
            del active[source_id]
        active[replacement.content.segment_id] = replacement.content
    staged_members = {item.event_key: item for segment in active.values() for item in segment.members}
    if set(staged_members) != set(members):
        raise ValueError('replacement changed the received originals')
    for key, member in staged_members.items():
        if member.raw_ref != key or member.revision != by_key[key].revision:
            raise ValueError(f'replacement evidence differs for {key}')

    disabled = None if view is None else view.disabled
    entries = [by_key[key] for key in sorted(staged_members)]
    rebuilt, rebuilt_contributions, rebuilt_totals, _ = build_version(entries, current.version, disabled)
    compare_snapshot_facts(current, rebuilt)
    contributions = load_contributions(db, current.version)
    totals = load_daily_totals(db, current.version)
    verify_reconciliation(current, contributions, totals)
    if rebuilt_contributions != contributions or rebuilt_totals != totals:
        raise ValueError('replacement changed the rebuilt daily ledger')
    catalog_version = publish_replacement(db, plan.source_version, replacements)
    retire_replaced_segments(db, replacements)
    edges = tuple(sorted((source_id, replacement.content.segment_id)
                         for replacement in replacements for source_id in replacement.source_ids))
    return PublishReceipt(request_id, 'compaction', result_id, current.version,
                          catalog_version=catalog_version, replacements=edges), False


def publish_compaction(db, request_id, start, end, source_version):
    validate_day(start)
    validate_day(end)
    validate_range(start, end)
    if type(source_version) is not int or source_version < 0:
        raise ValueError('source_version must be a nonnegative integer')
    return _run_publication(db, request_id, 'compaction', [start, end, source_version],
                            lambda: _publish_compaction(db, request_id, start, end, source_version))
