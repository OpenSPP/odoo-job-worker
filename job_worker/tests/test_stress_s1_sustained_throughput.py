"""Stress S1 — Sustained throughput (Tier 1).

Enqueue 5,000 trivial jobs and drain them via batched ``run_now()`` on a
single thread. Catches O(n) regressions in ``enqueue()`` or ``run_now()``
that the existing 20-job smoke would never surface. Does NOT exercise
the worker loop — that is S3's job.

Opt-in via ``--test-tags=stress``.
"""

import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    assert_queue_invariants,
    percentile,
    pg_version,
    record_report,
    setup_clean_queue,
)

TOTAL_JOBS = 5000
CHUNK_SIZE = 100


@tagged("post_install", "-at_install", "-standard", "stress")
class TestS1SustainedThroughput(TransactionCase):
    def setUp(self):
        super().setUp()
        setup_clean_queue(self)

    def test_s1_sustained_throughput_5000_jobs(self):
        channel = f"stress_s1_{uuid.uuid4().hex[:8]}"
        domain = [("id", "=", SUPERUSER_ID)]

        # Phase 1: enqueue. Single cursor, single commit.
        enqueue_started = time.monotonic()
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="res.users",
                    method_name="search",
                    record_ids=[],
                    args=[domain],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()
        enqueue_elapsed = time.monotonic() - enqueue_started

        # Phase 2: drain. Chunked run_now(). Per-chunk timings feed
        # percentile reporting; we do not assert against absolute
        # thresholds (soft target is relative-to-baseline per §8).
        chunk_timings = []
        drain_started = time.monotonic()
        done_total = 0
        while done_total < TOTAL_JOBS:
            chunk_started = time.monotonic()
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                batch = env["queue.job"].search(
                    [("channel", "=", channel), ("state", "=", "pending")],
                    limit=CHUNK_SIZE,
                )
                if not batch:
                    break
                batch.run_now()
                cr.commit()
            chunk_timings.append((time.monotonic() - chunk_started) * 1000)
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                done_total = env["queue.job"].search_count(
                    [("channel", "=", channel), ("state", "=", "done")]
                )

        drain_elapsed = time.monotonic() - drain_started

        # Hard assertions: correctness only.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            self.assertEqual(
                done, TOTAL_JOBS, f"expected {TOTAL_JOBS} done, got {done}"
            )
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        # Soft assertions: report only. Baseline comparison happens
        # offline against scripts/stress/_baseline.json.
        record_report(
            "S1",
            {
                "jobs": {
                    "enqueued": TOTAL_JOBS,
                    "done": done,
                    "failed": 0,
                    "cancelled": 0,
                },
                "elapsed_seconds": {
                    "enqueue": round(enqueue_elapsed, 3),
                    "drain": round(drain_elapsed, 3),
                    "total": round(enqueue_elapsed + drain_elapsed, 3),
                },
                "throughput_jobs_per_sec": round(TOTAL_JOBS / drain_elapsed, 2)
                if drain_elapsed > 0
                else None,
                "chunk_size": CHUNK_SIZE,
                "chunk_latency_ms": {
                    "p50": round(percentile(chunk_timings, 50), 2),
                    "p95": round(percentile(chunk_timings, 95), 2),
                    "p99": round(percentile(chunk_timings, 99), 2),
                    "samples": len(chunk_timings),
                },
                "pg_version": pg_version(self.env.registry),
            },
        )
