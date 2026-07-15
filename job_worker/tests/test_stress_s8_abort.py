"""Stress S8a/S8b — Abort during execution (Tier 2).

S8a — Graceful shutdown via SIGTERM. Per ``docs/deployment.md``: the
runner finishes running jobs, then exits. In-flight jobs are allowed
to complete naturally and end in ``done``; queued-but-not-started
jobs stay in ``pending``. A subsequent worker picks the pending ones
up. There is no auto-release back to pending on SIGTERM — operators
who need to stop quickly for a long-running job must use SIGKILL,
which S8b covers via the stale-heartbeat reclaim path.

S8b — Hard kill via SIGKILL. In-flight rows stay 'started' with stale
heartbeat. After ``stale_after_seconds`` (default 60s), a new worker
reclaims them via the acquire-stale path and they reach ``done``.

Depends on ``job_worker_stress`` (sleep_for body). Tagged ``tier2``;
opt-in via ``--test-tags=tier2``.
"""

import signal
import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    assert_queue_invariants,
    pg_version,
    record_report,
    runner_subprocess,
    setup_clean_queue,
    spawn_runner,
    terminate_runner,
    wait_for_started_count,
    wait_for_terminal_count,
)

TOTAL_JOBS = 20
CONCURRENCY = 4
# 3s is short enough that the test finishes quickly while still being
# long enough that the test process can win the race to send SIGTERM
# before any in-flight job naturally completes.
SLEEP_SECONDS = 3
# Generous: SIGTERM → wait_for_in_flight (~3s) → process exit + reads.
SHUTDOWN_TIMEOUT = 30


@tagged("post_install", "-at_install", "-standard", "tier2")
class TestS8aSigtermAbort(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest("job_worker_stress addon required for Tier 2")
        setup_clean_queue(self)

    def test_s8a_sigterm_waits_for_in_flight_then_exits(self):
        channel = f"stress_s8a_{uuid.uuid4().hex[:8]}"

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create({"name": channel, "limit": CONCURRENCY + 4})
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[SLEEP_SECONDS],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()

        # Phase 1: spawn one worker, wait until 4 rows are 'started',
        # then SIGTERM. The shutdown contract (deployment.md) is that
        # the runner waits for running jobs to finish, then exits.
        proc = spawn_runner(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
            env_overrides={"QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK": "0"},
        )
        try:
            wait_for_started_count(
                self.env.registry,
                channel=channel,
                minimum=CONCURRENCY,
                timeout=30,
            )
            with self.env.registry.cursor() as cr:
                cr.execute(
                    "SELECT DISTINCT worker_id FROM queue_job "
                    "WHERE channel = %s AND state = 'started'",
                    (channel,),
                )
                first_worker_ids = {row[0] for row in cr.fetchall()}
            self.assertEqual(
                len(first_worker_ids),
                1,
                f"expected single first worker, saw {first_worker_ids}",
            )
            first_worker_id = next(iter(first_worker_ids))
        finally:
            shutdown_started = time.monotonic()
            terminate_runner(
                proc,
                sig=signal.SIGTERM,
                graceful_timeout=SHUTDOWN_TIMEOUT,
            )
            shutdown_elapsed = time.monotonic() - shutdown_started

        # Phase 2: assert that the SIGTERM honoured the documented
        # contract — in-flight jobs ran to completion.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            cr.execute(
                "SELECT state, COUNT(*) FROM queue_job "
                "WHERE channel = %s GROUP BY state",
                (channel,),
            )
            states = {row[0]: row[1] for row in cr.fetchall()}

        # No row left mid-flight: documented contract is that the
        # runner waits before exiting, so 'started' must be empty.
        self.assertEqual(
            states.get("started", 0),
            0,
            f"expected 0 'started' rows after graceful shutdown, found "
            f"{states.get('started', 0)}. Full breakdown: {states}",
        )
        # The 4 that were in-flight should have completed; the other
        # 16 should still be pending (they were never acquired).
        self.assertGreaterEqual(
            states.get("done", 0),
            CONCURRENCY,
            f"expected at least {CONCURRENCY} done after graceful "
            f"shutdown (in-flight jobs allowed to finish). Breakdown: {states}",
        )
        # And shutdown should complete reasonably quickly — bounded by
        # the longest in-flight sleep + a few seconds for the runner's
        # poll loop and pool teardown.
        self.assertLess(
            shutdown_elapsed,
            SLEEP_SECONDS + 15,
            f"shutdown took {shutdown_elapsed:.1f}s — slower than "
            f"expected (~{SLEEP_SECONDS + 5}s for in-flight sleep + "
            f"teardown overhead)",
        )

        # Phase 3: spawn a fresh worker and assert it picks up the
        # remaining pending jobs and drains them to done.
        with runner_subprocess(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
            env_overrides={"QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK": "0"},
        ):
            drain_elapsed = wait_for_terminal_count(
                self.env.registry,
                channel=channel,
                expected=TOTAL_JOBS,
                timeout=60,
            )
            with self.env.registry.cursor() as cr:
                cr.execute(
                    "SELECT DISTINCT worker_id FROM queue_job "
                    "WHERE channel = %s AND state = 'started'",
                    (channel,),
                )
                second_worker_ids = {row[0] for row in cr.fetchall()}

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            self.assertEqual(done, TOTAL_JOBS)
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S8a",
            {
                "jobs": {"enqueued": TOTAL_JOBS, "done": done},
                "concurrency": CONCURRENCY,
                "post_sigterm_state": states,
                "first_worker_id": first_worker_id,
                "second_worker_ids": list(second_worker_ids - {first_worker_id}),
                "shutdown_seconds": round(shutdown_elapsed, 3),
                "recovery_drain_seconds": round(drain_elapsed, 3),
                "pg_version": pg_version(self.env.registry),
            },
        )


@tagged("post_install", "-at_install", "-standard", "tier2")
class TestS8bSigkillAbort(TransactionCase):
    """SIGKILL leaves rows in 'started' with stale heartbeat; another
    worker reclaims them after stale_after_seconds.

    This is the recommended escape hatch when SIGTERM would block for
    too long on a long-running in-flight job.
    """

    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest("job_worker_stress addon required for Tier 2")
        setup_clean_queue(self)

    def test_s8b_sigkill_recovered_via_stale_heartbeat(self):
        channel = f"stress_s8b_{uuid.uuid4().hex[:8]}"

        job_sleep = 2

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create({"name": channel, "limit": CONCURRENCY + 4})
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[job_sleep],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()

        # Phase 1: spawn worker, wait until in-flight, SIGKILL.
        proc = spawn_runner(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
            env_overrides={
                "QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK": "0",
            },
        )
        try:
            wait_for_started_count(
                self.env.registry,
                channel=channel,
                minimum=CONCURRENCY,
                timeout=30,
            )
        finally:
            terminate_runner(
                proc,
                sig=signal.SIGKILL,
                graceful_timeout=2,
                kill_timeout=5,
            )

        # Phase 2: immediately after SIGKILL, some rows are still
        # 'started' with stale-ish heartbeats — the worker had no
        # chance to clean them up.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env.cr.execute(
                "SELECT COUNT(*) FROM queue_job "
                "WHERE channel = %s AND state = 'started'",
                (channel,),
            )
            stuck_after_kill = env.cr.fetchone()[0]
        self.assertGreater(
            stuck_after_kill,
            0,
            f"expected some 'started' rows after SIGKILL, got {stuck_after_kill}",
        )

        # Phase 3: spawn a recovery worker. Stale-heartbeat reclaim
        # path (default 60s) should move jobs back to acquirable state
        # and they should complete.
        with runner_subprocess(
            self.env.cr.dbname,
            concurrency=CONCURRENCY,
            env_overrides={
                "QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK": "0",
            },
        ):
            drain_elapsed = wait_for_terminal_count(
                self.env.registry,
                channel=channel,
                expected=TOTAL_JOBS,
                timeout=120,
            )

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done = env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
            self.assertEqual(done, TOTAL_JOBS)
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S8b",
            {
                "jobs": {"enqueued": TOTAL_JOBS, "done": done},
                "stuck_after_sigkill": stuck_after_kill,
                "recovery_drain_seconds": round(drain_elapsed, 3),
                "pg_version": pg_version(self.env.registry),
            },
        )
