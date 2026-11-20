from collections import defaultdict
from heapq import heapify, heappop, heappush


class ReplayOrderError(ValueError):
    pass


def index_by_identity(revisions):
    by_identity = {}
    for item in revisions:
        prior = by_identity.setdefault(item.identity, item)
        if prior != item:
            raise ReplayOrderError(f'conflicting revision for {item.identity.key}')
    return by_identity


def validate_predecessors(by_identity):
    for identity, revision in by_identity.items():
        for required in revision.requires:
            if required not in by_identity:
                raise ReplayOrderError(f'{identity.key} requires missing predecessor {required.key}')


def require_unique_sequences(by_identity):
    seen = {}
    for identity, item in by_identity.items():
        slot = (identity.field_key, item.seq)
        if slot in seen:
            raise ReplayOrderError(f'sequence conflict on {identity.field_key} at {item.seq}')
        seen[slot] = identity


def order_effective_events(revisions):
    by_identity = index_by_identity(revisions)
    validate_predecessors(by_identity)
    require_unique_sequences(by_identity)

    by_field = defaultdict(dict)
    parents = {}
    for identity, item in by_identity.items():
        positions = by_field[identity.field_key]
        positions[item.seq] = identity
        parents[identity] = set(item.requires)
    for positions in by_field.values():
        ordered = [positions[seq] for seq in sorted(positions)]
        for earlier, later in zip(ordered, ordered[1:]):
            parents[later].add(earlier)

    successors = defaultdict(list)
    for child, predecessors in parents.items():
        for predecessor in predecessors:
            successors[predecessor].append(child)

    return _topological_order(by_identity, parents, successors)


def order_participants(revisions):
    """Order participating events with the V1 rules over the participant set.

    Field-sequence edges are still built inside each entity field, but only
    between participants: a paused neighbour is simply not a node, so the
    ordering relation it would have contributed cannot block anyone.
    """
    by_identity = index_by_identity(revisions)
    by_field = defaultdict(dict)
    parents = {}
    for identity, item in by_identity.items():
        positions = by_field[identity.field_key]
        positions[item.seq] = identity
        parents[identity] = set(item.requires)
    for positions in by_field.values():
        ordered = [positions[seq] for seq in sorted(positions)]
        for earlier, later in zip(ordered, ordered[1:]):
            parents[later].add(earlier)
    if any(parent not in by_identity for edges in parents.values() for parent in edges):
        raise ReplayOrderError('participant requires a non-participating predecessor')
    successors = defaultdict(list)
    for child, predecessors in parents.items():
        for predecessor in predecessors:
            successors[predecessor].append(child)
    return _topological_order(by_identity, parents, successors)


def _topological_order(by_identity, parents, successors):
    def priority(identity):
        return (by_identity[identity].seq, identity.entity, identity.field,
                identity.source, identity.event_id, identity)

    ready = [priority(identity) for identity, edges in parents.items() if not edges]
    heapify(ready)
    ordered = []
    while ready:
        identity = heappop(ready)[-1]
        ordered.append(by_identity[identity])
        for successor in successors[identity]:
            parents[successor].remove(identity)
            if not parents[successor]:
                heappush(ready, priority(successor))
    if len(ordered) != len(by_identity):
        raise ReplayOrderError('explicit predecessors conflict with the effective order')
    return tuple(ordered)
