"""Whole-received-set validation and disablement propagation.

Validation always covers every accepted original of the version, so a paused
event can never hide a missing predecessor, a sequence collision or a cycle.
Disablement then travels only along explicit ``requires`` edges; ordering by
field sequence determines replay order but never blocks a successor.
"""

from .order import order_effective_events


def validate_received(revisions):
    """Reject missing predecessors, same-field sequence conflicts and cycles."""
    return order_effective_events(revisions)


def blocked_sources_by_event(revisions, disabled):
    """Map each event identity to the sorted union of disabling sources.

    The union contains the event's own source when disabled, plus every
    blocked source of each explicit predecessor.  Ordering-only sequence
    neighbours are never consulted.
    """
    by_identity = {}
    for revision in revisions:
        by_identity.setdefault(revision.identity, revision)
    if len(by_identity) != len(tuple(revisions)):
        raise ValueError('received events contain duplicate identities')

    disabled = frozenset(disabled)
    resolved = {}
    for root in by_identity:
        if root in resolved:
            continue
        visiting = set()
        stack = [(root, False)]
        while stack:
            identity, expanded = stack.pop()
            if identity in resolved:
                continue
            revision = by_identity[identity]
            if expanded:
                visiting.discard(identity)
                blocked = set()
                if identity.source in disabled:
                    blocked.add(identity.source)
                for predecessor in revision.requires:
                    if predecessor not in resolved:
                        raise ValueError(f'{identity.key} depends on unresolved {predecessor.key}')
                    blocked.update(resolved[predecessor])
                resolved[identity] = tuple(sorted(blocked))
            else:
                if identity in visiting:
                    raise ValueError(f'predecessor cycle through {identity.key}')
                visiting.add(identity)
                stack.append((identity, True))
                for predecessor in revision.requires:
                    if predecessor not in resolved:
                        if predecessor not in by_identity:
                            raise ValueError(f'{identity.key} requires missing predecessor '
                                             f'{predecessor.key}')
                        stack.append((predecessor, False))
    return resolved
