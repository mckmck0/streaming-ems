"""AC-001-13/-16: DLQ records carry their error; validate reports clean topics."""

from __future__ import annotations

from typing import Any

from ems.contracts.examples import golden_examples
from ems.ingest.kafka.sink import KafkaSink
from ems.ingest.tools.reader import read_topic
from ems.ingest.tools.validate import validate_records


def test_ac_001_13_16_publish_validate_and_dlq(
    bootstrap: str, run_tag: str, topics_factory: Any
) -> None:
    prefix = f"{run_tag}."
    plan = topics_factory("sim", prefix)
    sink = KafkaSink(bootstrap, topic_prefix=prefix)
    examples = golden_examples()
    for event in examples.values():
        assert sink.publish(event)
    bad = examples["production.json"].model_copy(update={"t": 7})
    assert not sink.publish(bad)
    stats = sink.close()
    assert stats.exit_code() == 0 and stats.delivered == 5 and stats.dlq == 1

    by_name = plan.by_name()
    for base in (
        "ems.production.v1",
        "ems.consumption.v1",
        "ems.weather.forecast.v1",
        "ems.price.v1",
    ):
        name = plan.name_for(base)
        report = validate_records(
            name, read_topic(bootstrap, name), by_name[name].partitions, prefix
        )
        assert report.clean and report.valid == 1, report.lines()

    dlq = list(read_topic(bootstrap, plan.name_for("ems.gen.dlq.v1")))
    assert len(dlq) == 1 and dlq[0].key == "PL-LOC-000001"
    assert "t must be" in dlq[0].headers["error"]


def test_fr_012_tail_follow_sees_new_records(
    bootstrap: str, run_tag: str, topics_factory: Any
) -> None:
    import threading
    import time

    prefix = f"{run_tag}."
    plan = topics_factory("sim", prefix)
    name = plan.name_for("ems.price.v1")

    def later() -> None:
        time.sleep(2)
        sink = KafkaSink(bootstrap, topic_prefix=prefix)
        sink.publish(golden_examples()["price.json"])
        sink.close()

    threading.Thread(target=later, daemon=True).start()
    got = list(read_topic(bootstrap, name, from_beginning=False, follow=True, max_records=1))
    assert len(got) == 1 and got[0].key == "PL:RCE"
