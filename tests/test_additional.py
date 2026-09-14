from __future__ import annotations

from datetime import UTC, datetime

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue


def confirmed_event(
    *,
    event_id: str,
    merchant_id: str = "m-a",
    event_time: str = "2026-07-24T13:00:05Z",
    arrival_time: str = "2026-07-24T13:00:06Z",
    amount: int = 10,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_time": event_time,
        "arrival_time": arrival_time,
        "amount": amount,
        "status": "CONFIRMED",
    }


def test_parse_utc_normalizes_offsets_and_rejects_naive_values(solution):
    parsed = solution.parse_utc("2026-07-24T10:00:05-03:00")
    assert parsed == datetime(2026, 7, 24, 13, 0, 5, tzinfo=UTC)

    with pytest.raises(ValueError, match="zona horaria"):
        solution.parse_utc("2026-07-24T13:00:05")


@pytest.mark.parametrize("size", [0, -1])
def test_assign_fixed_window_rejects_non_positive_size(solution, size):
    timestamp = datetime(2026, 7, 24, 13, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="mayor que cero"):
        solution.assign_fixed_window(timestamp, size)


def test_unconfirmed_event_does_not_reserve_duplication_id(solution):
    pending = {
        **confirmed_event(event_id="same"),
        "status": "PENDING",
    }
    confirmed = confirmed_event(event_id="same", amount=25)

    totals, audit = solution.summarize_payments([pending, confirmed])

    assert [row["total"] for row in totals] == [25]
    assert [row["reason"] for row in audit] == [
        "not_confirmed",
        "accepted",
    ]


def test_same_event_id_in_different_windows_has_independent_state(solution):
    first = confirmed_event(event_id="shared", amount=10)
    second = confirmed_event(
        event_id="shared",
        event_time="2026-07-24T13:01:05Z",
        arrival_time="2026-07-24T13:01:06Z",
        amount=20,
    )

    totals, _ = solution.summarize_payments([first, second])

    assert [row["total"] for row in totals] == [10, 20]


def test_stateful_dofn_drops_duplicate_within_same_key_and_window(solution):
    duplicate = ("m-a", {"event_id": "same"})

    with BeamTestPipeline() as pipeline:
        output = (
            pipeline
            | beam.Create([duplicate, duplicate])
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(solution.DeduplicatePayments())
        )
        assert_that(output, equal_to([duplicate]))


def test_idempotent_sink_keeps_latest_revision(solution):
    original = {
        "merchant_id": "m-a",
        "window_start": "2026-07-24T13:00:00+00:00",
        "window_end": "2026-07-24T13:01:00+00:00",
        "total": 10,
    }
    revision = {**original, "total": 30}

    materialized, audit = solution.simulate_sink_retries(
        [original, revision],
        attempts=2,
        idempotent=True,
    )

    assert len(audit) == 4
    assert len(materialized) == 1
    assert materialized[0]["total"] == 30


@pytest.mark.parametrize("attempts", [0, -1])
def test_sink_rejects_non_positive_attempt_count(solution, attempts):
    with pytest.raises(ValueError, match="al menos 1"):
        solution.simulate_sink_retries([], attempts=attempts)


def test_teststream_accepts_late_event_within_allowed_lateness(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(("m-a", 10), 5)])
        .advance_watermark_to(60)
        .add_elements([TimestampedValue(("m-a", 20), 10)])
        .advance_watermark_to_infinity()
    )

    def check_panes(actual):
        assert sorted(total for _, total in actual) == [10, 30]

    options = PipelineOptions(streaming=True)
    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | stream
            | solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | beam.CombinePerKey(sum)
        )
        assert_that(output, check_panes)
