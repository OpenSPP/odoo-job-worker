"""Stress S6 — Retry storm (Tier 1).

500 jobs, each calling ``job.worker.stress.helper.retry_once_then_succeed``:
first attempt raises ``RetryableJobError(seconds=0)``, second succeeds.
Catches state-machine bugs that only manifest when many retry-scheduled
jobs become eligible simultaneously.

Depends on the ``job_worker_stress`` addon being installed.

Opt-in via ``--test-tags=stress``.
"""

import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    assert_queue_invariants,
    clear_queue,
    pg_version,
    record_report,
)

TOTAL_JOBS = 500
CHUNK_SIZE = 100


@tagged("post_install", "-at_install", "-standard", "stress")
class TestS6RetryStorm(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest(
                "job_worker_stress addon not installed — install it to run "
                "stress scenarios that need test-only job bodies"
            )
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            clear_queue(env)
            env["job.worker.stress.counter"].search([]).unlink()
            cr.commit()

    def test_s6_retry_storm_500_jobs(self):
        run_token = uuid.uuid4().hex[:8]
        channel = f"stress_s6_{run_token}"

        # Phase 1: enqueue 500 jobs, one per unique counter key.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            for i in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="retry_once_then_succeed",
                    record_ids=[],
                    args=[f"{run_token}_{i}"],
                    kwargs={},
                    channel=channel,
                    max_retries=5,
                )
            cr.commit()

        # Phase 2: drain. Each batch will see a mix of first-attempt
        # (scheduled_at=NULL) and just-retried (scheduled_at=now) jobs.
        # Per QueueJob._order = "priority ASC, scheduled_at ASC NULLS LAST,
        # id ASC", non-null scheduled_at sorts before NULL — so retried
        # jobs are processed *before* remaining originals once any retry
        # has happened.
        drain_started = time.monotonic()
        terminal = 0
        iterations = 0
        max_iterations = 50  # safety bound; expected ~10 iterations
        while terminal < TOTAL_JOBS and iterations < max_iterations:
            iterations += 1
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
            with self.env.registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                terminal = env["queue.job"].search_count(
                    [
                        ("channel", "=", channel),
                        ("state", "in", ["done", "failed", "cancelled"]),
                    ]
                )
        drain_elapsed = time.monotonic() - drain_started

        # Hard assertions.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done_jobs = env["queue.job"].search(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            failed_count = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "failed")]
            )
            pending_count = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "pending")]
            )

            self.assertEqual(len(done_jobs), TOTAL_JOBS, f"expected {TOTAL_JOBS} done")
            self.assertEqual(failed_count, 0, "no jobs should be failed")
            self.assertEqual(pending_count, 0, "no jobs should still be pending")

            # attempts == 1 on every job (one RetryableJobError each).
            # The design originally said sum=1000 (×2); corrected to 500
            # because attempts is incremented on retry only, not success.
            attempts_sum = sum(done_jobs.mapped("attempts"))
            self.assertEqual(
                attempts_sum,
                TOTAL_JOBS,
                f"expected sum(attempts) = {TOTAL_JOBS} (1 per job), got {attempts_sum}",
            )
            wrong_attempt_jobs = done_jobs.filtered(lambda j: j.attempts != 1)
            self.assertFalse(
                wrong_attempt_jobs,
                f"{len(wrong_attempt_jobs)} jobs had attempts != 1: "
                f"{wrong_attempt_jobs[:5].mapped('attempts')}",
            )

            # Counter rows: 500 keys with count=2 each.
            counters = env["job.worker.stress.counter"].search([])
            self.assertEqual(len(counters), TOTAL_JOBS)
            wrong_count = counters.filtered(lambda c: c.count != 2)
            self.assertFalse(
                wrong_count,
                f"{len(wrong_count)} counters had count != 2",
            )

            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S6",
            {
                "jobs": {
                    "enqueued": TOTAL_JOBS,
                    "done": len(done_jobs),
                    "failed": failed_count,
                    "pending": pending_count,
                },
                "elapsed_seconds": {"drain": round(drain_elapsed, 3)},
                "drain_iterations": iterations,
                "attempts_sum": attempts_sum,
                "throughput_jobs_per_sec": round(
                    TOTAL_JOBS / drain_elapsed, 2
                ) if drain_elapsed > 0 else None,
                "pg_version": pg_version(self.env.registry),
            },
        )
