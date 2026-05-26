"""Stress S7 — Timeout enforcement under load (Tier 2).

50 jobs with timeout=1 and max_retries=0, each calling sleep_for(5).
Single worker with concurrency=10. Every job must end in 'failed' with
``TimeoutJobError`` in exc_info; none stuck in 'started'.

Depends on ``job_worker_stress`` (sleep_for body). Tagged ``tier2``;
opt-in via ``--test-tags=tier2``.
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
    runner_subprocess,
    wait_for_terminal_count,
)

# Worker's heartbeat interval is 15s (default). The timeout is detected
# inside _heartbeat_loop on each heartbeat tick. For the heartbeat tick
# to actually catch the timeout, JOB_SLEEP must be > heartbeat_interval.
# Use 25s so the first heartbeat (at 15s) detects the timeout, then the
# pool thread completes its sleep at 25s and frees its slot.
#
# max_retries=1 means "1 retry, total 2 attempts" (per the docstring,
# max_retries=0 would be INFINITE retries). After the second timeout
# the job ends in 'failed' state with TimeoutJobError exc_info.
#
# Per-job wall: ~25s × 2 attempts + ~10s backoff between = ~60s.
TOTAL_JOBS = 10
JOB_TIMEOUT = 1
JOB_SLEEP_SECONDS = 25
JOB_MAX_RETRIES = 1
CONCURRENCY = 10
DRAIN_TIMEOUT_SECONDS = 180


@tagged("post_install", "-at_install", "-standard", "tier2")
class TestS7TimeoutUnderLoad(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest("job_worker_stress addon required for Tier 2")
        with self.env.registry.cursor() as cr:
            clear_queue(api.Environment(cr, SUPERUSER_ID, {}))
            cr.commit()

    def test_s7_timeout_kicks_in_under_load(self):
        channel = f"stress_s7_{uuid.uuid4().hex[:8]}"

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create({"name": channel, "limit": CONCURRENCY + 4})
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[JOB_SLEEP_SECONDS],
                    kwargs={},
                    channel=channel,
                    timeout=JOB_TIMEOUT,
                    max_retries=JOB_MAX_RETRIES,
                )
            cr.commit()

        start = time.monotonic()
        with runner_subprocess(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
        ):
            try:
                drain_elapsed = wait_for_terminal_count(
                    self.env.registry,
                    channel=channel,
                    expected=TOTAL_JOBS,
                    timeout=DRAIN_TIMEOUT_SECONDS,
                )
            except TimeoutError as err:
                self.fail(f"S7 drain timeout: {err}")
        total_elapsed = time.monotonic() - start

        # Every job should be in 'failed' with TimeoutJobError; none
        # stuck in 'started'.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            failed_jobs = env["queue.job"].search(
                [("channel", "=", channel), ("state", "=", "failed")]
            )
            done = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            stuck = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "started")]
            )

            self.assertEqual(
                len(failed_jobs), TOTAL_JOBS,
                f"expected {TOTAL_JOBS} failed, got {len(failed_jobs)}",
            )
            self.assertEqual(done, 0, "no jobs should reach 'done' on timeout")
            self.assertEqual(stuck, 0, "no jobs should remain 'started'")

            timeout_failures = failed_jobs.filtered(
                lambda j: j.exc_info and "TimeoutJobError" in j.exc_info
            )
            self.assertEqual(
                len(timeout_failures), TOTAL_JOBS,
                f"only {len(timeout_failures)}/{TOTAL_JOBS} jobs had "
                f"TimeoutJobError in exc_info",
            )

            # Each timed-out job's duration should be ≈ JOB_TIMEOUT
            # (within 1s slack — heartbeat-thread detects the timeout
            # asynchronously after the heartbeat interval).
            durations = [j.duration for j in failed_jobs if j.duration is not None]
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S7",
            {
                "jobs": {"enqueued": TOTAL_JOBS, "failed": len(failed_jobs)},
                "timeout_seconds": JOB_TIMEOUT,
                "job_sleep_seconds": JOB_SLEEP_SECONDS,
                "worker_concurrency": CONCURRENCY,
                "elapsed_seconds": {
                    "drain": round(drain_elapsed, 3),
                    "total": round(total_elapsed, 3),
                },
                "duration_field_samples": len(durations),
                "duration_avg_seconds": (
                    round(sum(durations) / len(durations), 3)
                    if durations else None
                ),
                "pg_version": pg_version(self.env.registry),
            },
        )
