"""Stress S2 — Burst enqueue under deep queue (Tier 1).

Builds a 10,000-job pending queue across 10 channels, then enqueues
another 1,000 in batches of 100 while measuring per-batch enqueue
latency, and finally measures the worker's ``acquire_job_lock`` SQL at
peak depth using ``EXPLAIN ANALYZE``. The scenario most likely to
catch missing-index regressions.

Drains the full 11,000 to ``done`` at the end so the FIFO-within-priority
invariant can be asserted.

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

CHANNELS = 10
JOBS_PER_CHANNEL_PREFILL = 1000
PREFILL_TOTAL = CHANNELS * JOBS_PER_CHANNEL_PREFILL  # 10,000
BURST_BATCHES = 10
BURST_BATCH_SIZE = 100
BURST_TOTAL = BURST_BATCHES * BURST_BATCH_SIZE  # 1,000
ALL_TOTAL = PREFILL_TOTAL + BURST_TOTAL  # 11,000

ACQUIRE_QUERY_SAMPLES = 20
DRAIN_CHUNK_SIZE = 500

# This is the same WHERE clause used by QueueWorker.acquire_job_lock at
# job_worker/cli/worker.py:297-341. Keep in sync. The point of this
# duplication is to time the *exact* shape of query the worker emits,
# so an EXPLAIN-ANALYZE here surfaces real-world regressions.
ACQUIRE_QUERY = """
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH fresh_running_counts AS (
    SELECT channel, COUNT(*) as running
    FROM queue_job
    WHERE state = 'started'
      AND heartbeat > NOW() - %s * INTERVAL '1 second'
    GROUP BY channel
),
recent_starts_counts AS (
    SELECT channel, COUNT(*) as started_count
    FROM queue_job
    WHERE (state = 'started' OR state = 'done')
      AND (heartbeat > NOW() - INTERVAL '1 second'
           OR write_date > NOW() - INTERVAL '1 second')
    GROUP BY channel
),
limits AS (
    SELECT name, "limit", rate_limit FROM queue_limit
)
SELECT j.id
FROM queue_job j
LEFT JOIN fresh_running_counts rc ON j.channel = rc.channel
LEFT JOIN recent_starts_counts rsc ON j.channel = rsc.channel
LEFT JOIN limits l ON j.channel = l.name
WHERE (
    j.state = 'pending'
    OR (
        j.state = 'started'
        AND (
            j.heartbeat IS NULL
            OR j.heartbeat < NOW() - %s * INTERVAL '1 second'
        )
    )
)
AND (j.scheduled_at IS NULL OR j.scheduled_at <= NOW())
AND (COALESCE(rc.running, 0) < COALESCE(l."limit", 1))
AND (l.rate_limit IS NULL OR l.rate_limit = 0
     OR COALESCE(rsc.started_count, 0) < l.rate_limit)
ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
"""


@tagged("post_install", "-at_install", "-standard", "stress")
class TestS2BurstUnderDepth(TransactionCase):
    def setUp(self):
        super().setUp()
        setup_clean_queue(self)

    def _enqueue(self, env, channel, count):
        domain = [("id", "=", SUPERUSER_ID)]
        for _ in range(count):
            env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[domain],
                kwargs={},
                channel=channel,
            )

    def test_s2_burst_under_depth_11000(self):
        run_token = uuid.uuid4().hex[:6]
        channels = [f"stress_s2_{run_token}_{i}" for i in range(CHANNELS)]

        # Phase 1: pre-fill 10,000 across 10 channels.
        prefill_started = time.monotonic()
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            for channel in channels:
                self._enqueue(env, channel, JOBS_PER_CHANNEL_PREFILL)
            cr.commit()
        prefill_elapsed = time.monotonic() - prefill_started

        # Phase 2: burst enqueue. 10 batches × 100 jobs each. Rotate
        # channel per batch so the burst spreads across the namespace.
        burst_batch_ms = []
        for batch_idx in range(BURST_BATCHES):
            channel = channels[batch_idx % CHANNELS]
            t0 = time.monotonic()
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                self._enqueue(env, channel, BURST_BATCH_SIZE)
                cr.commit()
            burst_batch_ms.append((time.monotonic() - t0) * 1000)

        # Phase 3: measure acquire query at peak depth (11,000 pending).
        # Run EXPLAIN ANALYZE N times and capture total execution time
        # from the JSON plan. Uses raw cursors (no ORM) — closer to the
        # worker's actual code path.
        acquire_ms = []
        stale_seconds = 60
        with self.env.registry.cursor() as cr:
            for _ in range(ACQUIRE_QUERY_SAMPLES):
                cr.execute(ACQUIRE_QUERY, (stale_seconds, stale_seconds))
                plan = cr.fetchone()[0]
                # plan is a list[dict]; "Execution Time" is in ms
                exec_time = plan[0].get("Execution Time", 0.0)
                acquire_ms.append(exec_time)
                cr.rollback()  # don't actually acquire — measurement only

        # Phase 4: drain via batched run_now(). Larger chunks than S1
        # because we already have S1 covering the small-chunk path —
        # here we just want the queue empty to assert invariants.
        drain_started = time.monotonic()
        done_total = 0
        while done_total < ALL_TOTAL:
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                batch = env["queue.job"].search(
                    [("channel", "in", channels), ("state", "=", "pending")],
                    limit=DRAIN_CHUNK_SIZE,
                )
                if not batch:
                    break
                batch.run_now()
                cr.commit()
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                done_total = env["queue.job"].search_count(
                    [("channel", "in", channels), ("state", "=", "done")]
                )
        drain_elapsed = time.monotonic() - drain_started

        # Hard assertions.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done = env["queue.job"].search_count(
                [("channel", "in", channels), ("state", "=", "done")]
            )
            self.assertEqual(done, ALL_TOTAL)
            assert_queue_invariants(self, env, expected_total=ALL_TOTAL)
            # Per-channel breakdown: confirm even distribution survived.
            for channel in channels:
                count = env["queue.job"].search_count(
                    [("channel", "=", channel), ("state", "=", "done")]
                )
                self.assertGreater(count, 0, f"channel {channel} had zero done jobs")

        record_report(
            "S2",
            {
                "jobs": {
                    "prefill": PREFILL_TOTAL,
                    "burst": BURST_TOTAL,
                    "total": ALL_TOTAL,
                    "done": done,
                },
                "channels": CHANNELS,
                "elapsed_seconds": {
                    "prefill_enqueue": round(prefill_elapsed, 3),
                    "drain": round(drain_elapsed, 3),
                },
                "burst_batch_latency_ms": {
                    "p50": round(percentile(burst_batch_ms, 50), 2),
                    "p95": round(percentile(burst_batch_ms, 95), 2),
                    "p99": round(percentile(burst_batch_ms, 99), 2),
                    "samples": len(burst_batch_ms),
                    "batch_size": BURST_BATCH_SIZE,
                },
                "acquire_query_latency_ms": {
                    "p50": round(percentile(acquire_ms, 50), 2),
                    "p95": round(percentile(acquire_ms, 95), 2),
                    "p99": round(percentile(acquire_ms, 99), 2),
                    "samples": len(acquire_ms),
                    "queue_depth_at_measurement": ALL_TOTAL,
                },
                "pg_version": pg_version(self.env.registry),
            },
        )
