"""Diagnostic: reproduce the worker-hang-at-scale and snapshot state.

Bumps S3 back up to the 2,000-job scale where workers stall after
processing ~1,150 jobs (~72 per pool slot at concurrency=4). When
progress flatlines for ``STALL_THRESHOLD`` consecutive seconds, the
test dumps everything it can:

- ``pg_stat_activity`` rows for the test DB: state, wait event, query
  start, the actual query text — shows whether backends are blocked on
  locks, idle, or active.
- Per-state ``queue_job`` counts: confirms the breakdown we observed
  (1,150-ish done / handful stuck started / rest pending).
- Each stuck row's worker_id, heartbeat, started_at — to tell who
  *thinks* it owns each stuck job.
- The active subprocess pids, so the investigator can attach to them.

Tagged ``diagnose`` and ``-standard`` — opt-in via
``--test-tags=diagnose``. Never runs in regular CI or with
``--test-tags=stress``.
"""

import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    runner_subprocess,
    setup_clean_queue,
)

TOTAL_JOBS = 2000
WORKER_COUNT = 4
CONCURRENCY_PER_WORKER = 4
EFFECTIVE_SLOTS = WORKER_COUNT * CONCURRENCY_PER_WORKER

# Drain target. We *expect* this to time out — that's the bug.
MAX_WALL_SECONDS = 180
# Poll cadence + stall detector.
POLL_INTERVAL_SECONDS = 1.0
STALL_THRESHOLD = 15  # 15 consecutive polls with no progress = "hung"


@tagged("post_install", "-at_install", "-standard", "diagnose")
class TestDiagnoseWorkerHang(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest("job_worker_stress addon required")
        setup_clean_queue(self)

    def test_diagnose_worker_hang_at_2000_jobs(self):
        channel = f"diagnose_hang_{uuid.uuid4().hex[:8]}"

        # Pre-create the queue.limit so the channel can fan out to 16
        # slots — keeping the only variable under test the worker's
        # own machinery, not the acquire SQL's COALESCE(limit, 1) trap.
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create({"name": channel, "limit": EFFECTIVE_SLOTS + 4})
            for _ in range(TOTAL_JOBS):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[0.01],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()

        env_no_lock = {"QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK": "0"}
        procs = []
        try:
            for _ in range(WORKER_COUNT):
                procs.append(
                    self.enterContext(
                        runner_subprocess(
                            self.env.cr.dbname,
                            concurrency=CONCURRENCY_PER_WORKER,
                            env_overrides=env_no_lock,
                        )
                    )
                )
            self._watch_until_stall_or_done(channel, [p.pid for p in procs])
        finally:
            pass  # context manager will SIGTERM the workers

    # --- internals ---

    def _watch_until_stall_or_done(self, channel, pids):
        start = time.monotonic()
        last_terminal = 0
        stall_count = 0
        deadline = start + MAX_WALL_SECONDS

        while time.monotonic() < deadline:
            terminal, started, pending = self._counts(channel)
            if terminal >= TOTAL_JOBS:
                self._log_info(
                    "Suite drained cleanly in %.1fs — bug not reproduced.",
                    time.monotonic() - start,
                )
                return
            if terminal == last_terminal:
                stall_count += 1
            else:
                stall_count = 0
                last_terminal = terminal
            if stall_count >= STALL_THRESHOLD:
                self._log_info(
                    "STALL DETECTED at done=%d started=%d pending=%d "
                    "after %.1fs (no progress for %d × %.1fs polls)",
                    terminal,
                    started,
                    pending,
                    time.monotonic() - start,
                    stall_count,
                    POLL_INTERVAL_SECONDS,
                )
                self._snapshot_pg_state(channel)
                self._snapshot_stuck_rows(channel)
                self._dump_worker_thread_stacks(pids)
                # Stop early — we have what we came for.
                return
            time.sleep(POLL_INTERVAL_SECONDS)

        self._log_info(
            "Exhausted MAX_WALL_SECONDS (%ds) — final counts done=%d "
            "started=%d pending=%d. Bug may not have triggered at this scale.",
            MAX_WALL_SECONDS,
            *self._counts(channel),
        )

    def _counts(self, channel):
        with self.env.registry.cursor() as cr:
            cr.execute(
                "SELECT state, COUNT(*) FROM queue_job WHERE channel = %s "
                "GROUP BY state",
                (channel,),
            )
            by_state = {row[0]: row[1] for row in cr.fetchall()}
        terminal = (
            by_state.get("done", 0)
            + by_state.get("failed", 0)
            + by_state.get("cancelled", 0)
        )
        return terminal, by_state.get("started", 0), by_state.get("pending", 0)

    def _snapshot_pg_state(self, channel):
        with self.env.registry.cursor() as cr:
            cr.execute(
                """
                SELECT pid,
                       application_name,
                       state,
                       wait_event_type,
                       wait_event,
                       EXTRACT(EPOCH FROM (NOW() - state_change)) AS state_age_s,
                       EXTRACT(EPOCH FROM (NOW() - query_start)) AS query_age_s,
                       LEFT(query, 200) AS query_snippet
                  FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND pid <> pg_backend_pid()
                 ORDER BY pid
                """
            )
            rows = cr.fetchall()
        self._log_info(
            "pg_stat_activity: %d backends connected to %s",
            len(rows),
            self.env.cr.dbname,
        )
        for r in rows:
            (pid, app, state, wet, we, sage, qage, q) = r
            self._log_info(
                "  pid=%-6s app=%-15s state=%-15s wait=%s/%s "
                "state_age=%-6.1fs query_age=%-6.1fs",
                pid,
                (app or "")[:15],
                state or "?",
                wet or "-",
                we or "-",
                sage or 0,
                qage or 0,
            )
            if q and q.strip():
                self._log_info("    query: %s", q.strip().replace("\n", " ")[:180])

    def _snapshot_stuck_rows(self, channel):
        with self.env.registry.cursor() as cr:
            cr.execute(
                """
                SELECT id,
                       state,
                       worker_id,
                       EXTRACT(EPOCH FROM (NOW() - heartbeat)) AS heartbeat_age_s,
                       EXTRACT(EPOCH FROM (NOW() - started_at)) AS started_age_s,
                       attempts
                  FROM queue_job
                 WHERE channel = %s
                   AND state = 'started'
                 ORDER BY id
                """,
                (channel,),
            )
            rows = cr.fetchall()
        self._log_info("stuck 'started' rows on channel %s: %d", channel, len(rows))
        for r in rows:
            _id, _state, _worker, _hb_age, _started_age, _attempts = r
            self._log_info(
                "  id=%s state=%s worker=%s hb_age=%.1fs started_age=%.1fs attempts=%s",
                _id,
                _state,
                _worker,
                _hb_age or 0,
                _started_age or 0,
                _attempts,
            )

    def _dump_worker_thread_stacks(self, pids):
        """Send SIGUSR1 to each worker; the runner's faulthandler dumps
        all-thread stacks to its stderr. That stderr is captured by
        runner_subprocess on shutdown and surfaced as a WARNING log line.
        """
        import os
        import signal as signal_mod

        self._log_info("Sending SIGUSR1 to %d worker pids for stack dump", len(pids))
        for pid in pids:
            try:
                os.kill(pid, signal_mod.SIGUSR1)
            except ProcessLookupError:
                self._log_info("  pid %s no longer alive", pid)
        # Give faulthandler a moment to write the stacks to stderr.
        time.sleep(2)
        self._log_info(
            "Stacks will appear in the 'Runner pid=X stderr (tail)' WARNING "
            "lines printed when the test exits the runner_subprocess context."
        )

    @staticmethod
    def _log_info(fmt, *args):
        # Use Odoo's logger so it goes through the test runner's stream.
        import logging

        logging.getLogger(__name__).info(fmt, *args)
