import json
from contextlib import closing
from pathlib import Path

from src.facts.order import ReplayOrderError
from src.partitions.catalog import load_catalog
from src.publish.coordinator import publish_batch, publish_compaction
from src.publish.database import initialize_database, open_database
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history


def main():
    sample = json.loads(Path(__file__).with_name("source_status.json").read_text(encoding="utf-8"))
    day = sample["day"]
    originals = sample["initial"]
    late_records = [originals[0], sample["late"]]
    with closing(open_database(":memory:")) as db:
        initialize_database(db)
        first = publish_batch(db, "base", originals)
        old = query_history(db, first.result_id)
        for name in ("conflict", "missing"):
            try:
                publish_batch(db, name, [sample[name]])
            except ReplayOrderError:
                if find_request_receipt(db, name) is not None or query_current(db) != old:
                    raise AssertionError(f"rejected {name} changed the ledger")
            else:
                raise AssertionError(f"accepted invalid {name} fixture")

        late = publish_batch(db, "late", late_records)
        current = query_current(db)
        if [item.total_mg for item in old.daily] != ["26"] or [item.total_mg for item in current.daily] != ["27"]:
            raise AssertionError("unexpected V1 totals for the source-status fixture")
        provenance = {item.event.event_id: (item.request_id, item.result_id) for item in late.events}
        if provenance != {"a": ("base", first.result_id), "late-a": ("late", late.result_id)}:
            raise AssertionError("mixed batch lost first-acceptance provenance")
        repeated = publish_batch(db, "repeat", late_records)
        if repeated.result_id is not None or repeated.events != late.events:
            raise AssertionError("pure replay changed first-acceptance provenance")

        maintenance = publish_compaction(db, "compact", day, day, load_catalog(db).version)
        if maintenance.result_id != late.result_id or query_current(db) != current:
            raise AssertionError("compaction changed the V1 ledger")
        if query_history(db, first.result_id) != old:
            raise AssertionError("compaction changed the old V1 result")
        print(json.dumps({"v1_initial_mg": old.daily[0].total_mg,
                          "v1_after_late_mg": current.daily[0].total_mg,
                          "first_receipts": provenance,
                          "compacted_result": maintenance.result_id}, sort_keys=True))


if __name__ == "__main__":
    main()
