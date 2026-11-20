import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ViewEvent:
    key: str
    accepted_request: str
    blocked: tuple[str, ...]

    @property
    def paused(self):
        return bool(self.blocked)


@dataclass(frozen=True)
class SourceView:
    version: int
    disabled: tuple[str, ...]
    events: tuple[ViewEvent, ...]


def initialize_views(db):
    db.execute('''CREATE TABLE IF NOT EXISTS source_versions (
        version INTEGER PRIMARY KEY REFERENCES fact_snapshots(version),
        disabled_json TEXT NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS source_version_events (
        version INTEGER NOT NULL REFERENCES source_versions(version),
        event_key TEXT NOT NULL, accepted_request TEXT NOT NULL, blocked_json TEXT NOT NULL,
        PRIMARY KEY (version, event_key)
    )''')


def save_view(db, view):
    db.execute('INSERT INTO source_versions VALUES (?, ?)',
               (view.version, json.dumps(list(view.disabled))))
    db.executemany('INSERT INTO source_version_events VALUES (?, ?, ?, ?)', [
        (view.version, item.key, item.accepted_request, json.dumps(list(item.blocked)))
        for item in view.events
    ])


def load_view(db, version):
    row = db.execute('SELECT disabled_json FROM source_versions WHERE version = ?', (version,)).fetchone()
    if row is None:
        return None
    disabled = json.loads(row[0])
    if (not isinstance(disabled, list) or any(not isinstance(item, str) for item in disabled)
            or disabled != sorted(set(disabled))):
        raise ValueError(f'invalid disabled-source tuple for version {version}')
    events = []
    for key, request_id, blocked_json in db.execute('''
            SELECT event_key, accepted_request, blocked_json FROM source_version_events
            WHERE version = ? ORDER BY event_key''', (version,)):
        blocked = json.loads(blocked_json)
        if (not isinstance(blocked, list) or any(not isinstance(item, str) for item in blocked)
                or blocked != sorted(set(blocked))):
            raise ValueError(f'invalid blocked-source tuple for {key} in version {version}')
        events.append(ViewEvent(key, request_id, tuple(blocked)))
    return SourceView(version, tuple(disabled), tuple(events))
