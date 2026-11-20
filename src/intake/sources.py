from .raw_store import accepted_sources


def initialize_source_status(db):
    db.execute('''CREATE TABLE IF NOT EXISTS source_status (
        source TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL CHECK (enabled IN (0, 1))
    )''')


def ensure_sources(db, sources):
    for source in sorted(set(sources)):
        db.execute('INSERT OR IGNORE INTO source_status VALUES (?, 1)', (source,))


def adopt_accepted_sources(db):
    """Register sources of originals already present in an older database."""
    ensure_sources(db, accepted_sources(db))


def load_source_status(db):
    return {source: bool(enabled)
            for source, enabled in db.execute('SELECT source, enabled FROM source_status ORDER BY source')}


def disabled_sources(status):
    return tuple(sorted(source for source, enabled in status.items() if not enabled))


def set_source_enabled(db, source, enabled):
    cursor = db.execute('UPDATE source_status SET enabled = ? WHERE source = ?',
                        (1 if enabled else 0, source))
    if cursor.rowcount != 1:
        raise ValueError(f'unknown source {source!r}')
