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
