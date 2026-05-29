"""Stress S5 — Channel rate limit enforcement under load (Tier 2).

queue.limit row for a channel says limit=100, rate_limit=10. Enqueue
100 trivial jobs to that channel. Spawn one worker with concurrency=10.
Within any 1-second wall-clock window, at most 10 jobs should
transition through state='started'.

The rate-limit logic is best-effort: the SQL window is "started or done
in last 1 second" and the check happens BEFORE the UPDATE that marks
'started'. So there is a small TOCTOU window where the limit can be
overshot. This test quantifies the overshoot — pure correctness assertion
is "total done == 100 in roughly 10s" and we report the observed peak
RPS so a regression that doubles overshoot surfaces.

Depends on ``job_worker_stress`` (sleep_for body). Tagged ``tier2``;
opt-in via ``--test-tags=tier2``.
"""

import threading
import uuid
from collections import deque

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    assert_queue_invariants,
    clear_queue,
    pg_version,
    record_report,
    runner_subprocess,
    wait_for_terminal_count,
)

TOTAL_JOBS = 100
JOB_SLEEP_SECONDS = 0.05  # short enough to test rate-limiting, not concurrency
CHANNEL_LIMIT = 100  # well above effective slots — concurrency not the limiter
CHANNEL_RATE_LIMIT = 10  # 10 jobs/sec
CONCURRENCY = 10
DRAIN_TIMEOUT_SECONDS = 60


@tagged("post_install", "-at_install", "-standard", "tier2")
class TestS5RateLimit(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest("job_worker_stress addon required for Tier 2")
        with self.env.registry.cursor() as cr:
            clear_queue(api.Environment(cr, SUPERUSER_ID, {}))
            cr.commit()

    def test_s5_rate_limit_caps_throughput(self):
        channel = f"stress_s5_{uuid.uuid4().hex[:8]}"

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create(
                {
                    "name": channel,
                    "limit": CHANNEL_LIMIT,
                    "rate_limit": CHANNEL_RATE_LIMIT,
                }
            )
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[JOB_SLEEP_SECONDS],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()

        # Sample distinct started_at timestamps every 50ms to estimate
        # the per-second start rate the worker actually achieves.
        sampler_stop = threading.Event()
        registry = self.env.registry
        observed_started_ids = set()
        lock = threading.Lock()

        def sample_loop():
            while not sampler_stop.wait(0.05):
                try:
                    with registry.cursor() as cr:
                        cr.execute(
                            "SELECT id FROM queue_job "
                            "WHERE channel = %s "
                            "  AND state IN ('started', 'done')",
                            (channel,),
                        )
                        ids = {row[0] for row in cr.fetchall()}
                    with lock:
                        observed_started_ids.update(ids)
                except Exception:  # noqa: BLE001
                    pass

        with runner_subprocess(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
        ):
            sampler = threading.Thread(target=sample_loop, daemon=True)
            sampler.start()
            try:
                drain_elapsed = wait_for_terminal_count(
                    self.env.registry,
                    channel=channel,
                    expected=TOTAL_JOBS,
                    timeout=DRAIN_TIMEOUT_SECONDS,
                )
            except TimeoutError as err:
                self.fail(f"S5 drain timeout: {err}")
            finally:
                sampler_stop.set()
                sampler.join(timeout=2)

        # Compute observed peak RPS from started_at distribution.
        with self.env.registry.cursor() as cr:
            cr.execute(
                "SELECT started_at FROM queue_job "
                "WHERE channel = %s AND started_at IS NOT NULL "
                "ORDER BY started_at",
                (channel,),
            )
            timestamps = [row[0] for row in cr.fetchall()]

        peak_rps = self._peak_rate_per_second(timestamps)

        # Hard assertion: 100 jobs at 10 rps should take >= ~9s.
        # Allow generous tolerance (8s) for clock skew + best-effort drift.
        self.assertGreaterEqual(
            drain_elapsed,
            (TOTAL_JOBS / CHANNEL_RATE_LIMIT) * 0.8,
            f"drain finished in {drain_elapsed:.2f}s — rate limit may not "
            f"have been enforced (expected ~10s for {TOTAL_JOBS} jobs at "
            f"{CHANNEL_RATE_LIMIT} rps)",
        )

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            self.assertEqual(done, TOTAL_JOBS)
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S5",
            {
                "jobs": {"enqueued": TOTAL_JOBS, "done": done},
                "channel_rate_limit": CHANNEL_RATE_LIMIT,
                "channel_concurrency_limit": CHANNEL_LIMIT,
                "worker_concurrency": CONCURRENCY,
                "drain_seconds": round(drain_elapsed, 3),
                "expected_min_drain_seconds": TOTAL_JOBS / CHANNEL_RATE_LIMIT,
                "peak_observed_rps": peak_rps,
                "pg_version": pg_version(self.env.registry),
            },
        )

    @staticmethod
    def _peak_rate_per_second(timestamps):
        """Sliding-window peak count within any 1-second wall-clock window."""
        if not timestamps:
            return 0
        from datetime import timedelta

        ordered = sorted(timestamps)
        window = deque()
        peak = 0
        for ts in ordered:
            window.append(ts)
            while window and (ts - window[0]) > timedelta(seconds=1):
                window.popleft()
            if len(window) > peak:
                peak = len(window)
        return peak
