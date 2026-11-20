"""Shared rebuild of facts, contributions and daily totals for a data version.

Every data version created by the source-status release stores the whole
received set in ``received_events`` and, once a status switch has happened,
also a status view in ``source_versions``.  A version without a view is V1:
all sources are enabled and the whole set takes part in replay.  The
publisher and the query side both use this module so publishing and re-reading
apply identical rules.
"""

from src.aggregate.contributions import derive_contributions
from src.aggregate.daily import build_daily_totals
from src.facts.eligibility import blocked_sources_by_event, validate_received
from src.facts.replay import replay_events, replay_participants
from src.facts.snapshot import load_snapshot
from src.facts.views import load_view
from src.intake.raw_store import load_raw_events


def initialize_received(db):
    db.execute('''CREATE TABLE IF NOT EXISTS received_events (
        version INTEGER NOT NULL REFERENCES fact_snapshots(version),
        event_key TEXT NOT NULL, PRIMARY KEY (version, event_key)
    )''')


def store_received(db, version, keys):
    db.executemany('INSERT INTO received_events VALUES (?, ?)',
                   [(version, key) for key in sorted(keys)])


def received_keys(db, version):
    return tuple(row[0] for row in db.execute(
        'SELECT event_key FROM received_events WHERE version = ? ORDER BY event_key', (version,)))


def version_received_keys(db, version):
    """Received originals of a version.

    Versions published before the source-status release have no received
    manifest; for them the V1 snapshot membership is the received set.  A
    qualified version, however, must keep its manifest: losing paused
    originals is a contradiction, not a fallback.
    """
    keys = received_keys(db, version)
    if keys:
        return keys
    if load_view(db, version) is not None:
        raise ValueError(f'no received manifest for qualified version {version}')
    return load_snapshot(db, version).members


def build_version(entries, version, disabled=None):
    """Return (snapshot, contributions, totals, blocked map).

    ``disabled`` is None for a V1 version.  Otherwise the whole received set is
    validated first and replay covers only events with no blocking source.
    """
    revisions = tuple(entry.revision for entry in entries)
    if disabled is None:
        replayed = replay_events(revisions, version,
                                 expected_keys={entry.raw.identity.key for entry in entries})
        snapshot = replayed.snapshot
        contributions = derive_contributions(snapshot)
        return snapshot, contributions, build_daily_totals(version, contributions), ()

    validate_received(revisions)
    blocked = blocked_sources_by_event(revisions, disabled)
    participants = tuple(item for item in revisions if not blocked[item.identity])
    snapshot = replay_participants(participants, version,
                                   expected_keys={item.identity.key for item in participants})
    contributions = derive_contributions(snapshot)
    totals = build_daily_totals(version, contributions)
    ordered = tuple(sorted(blocked.items(), key=lambda pair: pair[0].key))
    return snapshot, contributions, totals, ordered


def rebuild_version(db, version):
    """Rebuild a stored version from its received originals and status view."""
    view = load_view(db, version)
    disabled = None if view is None else view.disabled
    entries = load_raw_events(db, version_received_keys(db, version))
    for entry in entries:
        if entry.accepted_version > version:
            raise ValueError(f'original {entry.raw.identity.key} was accepted after version {version}')
    return build_version(entries, version, disabled)
