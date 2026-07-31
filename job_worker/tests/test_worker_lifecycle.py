import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerLifecycle(TransactionCase):
    def test_run_exits_immediately_when_stop_event_is_set(self):
        stop_event = threading.Event()
        stop_event.set()
        worker = QueueWorker(self.env.cr.dbname, stop_event=stop_event, poll_timeout=0)

        with (
            patch.object(worker, "process_jobs") as process_jobs,
            patch.object(worker, "update_heartbeats") as update_heartbeats,
        ):
            worker.run()

        process_jobs.assert_not_called()
        update_heartbeats.assert_not_called()

    def test_run_loop_completes_single_cycle_without_hanging(self):
        stop_event = threading.Event()
        worker = QueueWorker(self.env.cr.dbname, stop_event=stop_event, poll_timeout=0)

        def process_once():
            stop_event.set()

        start = time.monotonic()
        with (
            patch.object(
                worker, "process_jobs", side_effect=process_once
            ) as process_jobs,
            patch.object(worker, "update_heartbeats") as update_heartbeats,
        ):
            worker.run()
        elapsed = time.monotonic() - start

        self.assertEqual(process_jobs.call_count, 1)
        self.assertEqual(update_heartbeats.call_count, 1)
        self.assertTrue(elapsed < 1, f"run loop should stop quickly, elapsed={elapsed}")

    def test_process_jobs_respects_stop_event_during_drain(self):
        stop_event = threading.Event()
        worker = QueueWorker(self.env.cr.dbname, stop_event=stop_event, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)
        call_count = {"value": 0}

        def acquire_and_stop():
            call_count["value"] += 1
            if call_count["value"] == 1:
                stop_event.set()
            return call_count["value"]

        with patch.object(worker, "_acquire_job", side_effect=acquire_and_stop):
            worker.process_jobs()

        self.assertEqual(call_count["value"], 1)

    def test_waiting_for_an_active_job_counts_as_progress(self):
        """A healthy worker running one slow job must not look stalled.

        The supervisor's stall watchdog reads ``last_progress``, which ``run()``
        advances only at the top of each main-loop cycle. ``process_jobs`` can
        legitimately hold the loop for as long as a job takes: one job active, a
        free pool slot, and nothing else acquirable (that job's channel is at its
        limit). Before this was fixed the timestamp froze there and the watchdog
        killed the process at ``worker_stall_timeout_seconds`` — the preprod
        entitlement-compute crash-loop, where concurrency=2 and a channel limit
        of 1 made the free-slot condition permanent.
        """
        stop_event = threading.Event()
        worker = QueueWorker(self.env.cr.dbname, stop_event=stop_event, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)
        # One job in flight, a free slot, and nothing acquirable: the exact shape
        # that used to freeze last_progress.
        worker.active_job_ids = {4242}
        stalled_since = time.monotonic() - 500
        worker.last_progress = stalled_since

        calls = {"value": 0}

        def nothing_acquirable():
            calls["value"] += 1
            if calls["value"] >= 3:
                stop_event.set()
            return None

        with patch.object(worker, "_acquire_job", side_effect=nothing_acquirable):
            worker.process_jobs()

        self.assertGreaterEqual(calls["value"], 3, "the wait branch never ran")
        self.assertGreater(
            worker.last_progress,
            stalled_since,
            "waiting on an active job left last_progress frozen, so the stall "
            "watchdog would kill a healthy worker running a slow job",
        )

    def test_draining_to_empty_does_not_touch_progress(self):
        """No active jobs and nothing acquirable is a normal empty-queue exit.

        It returns to run(), which advances last_progress itself, so this branch
        must not need to — asserting that keeps the fix scoped to the branch that
        can actually hold the loop.
        """
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)
        worker.active_job_ids = set()
        stalled_since = time.monotonic() - 500
        worker.last_progress = stalled_since

        with patch.object(worker, "_acquire_job", return_value=None):
            worker.process_jobs()

        self.assertEqual(worker.last_progress, stalled_since)
