import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerConcurrency(TransactionCase):
    """Tests for per-worker thread pool concurrency."""

    def test_default_concurrency_is_two(self):
        worker = QueueWorker(self.env.cr.dbname)
        self.assertEqual(worker.concurrency, 2)

    def test_concurrency_clamped_to_minimum_one(self):
        worker_zero = QueueWorker(self.env.cr.dbname, concurrency=0)
        self.assertEqual(worker_zero.concurrency, 1)

        worker_negative = QueueWorker(self.env.cr.dbname, concurrency=-1)
        self.assertEqual(worker_negative.concurrency, 1)

    def test_has_active_lock(self):
        worker = QueueWorker(self.env.cr.dbname)
        self.assertIsInstance(worker._active_lock, type(threading.Lock()))

    def test_process_jobs_stops_at_concurrency_limit(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)
        # Pre-fill active jobs to capacity
        with worker._active_lock:
            worker.active_job_ids.update({100, 200})

        with patch.object(worker, "_acquire_job") as mock_acquire:
            worker.process_jobs()
            mock_acquire.assert_not_called()

    def test_process_jobs_stops_when_no_jobs(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)

        with patch.object(worker, "_acquire_job", return_value=None) as mock_acquire:
            worker.process_jobs()
            mock_acquire.assert_called_once()

    def test_process_jobs_submits_to_pool(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)

        acquire_results = iter([10, 20, None])
        with patch.object(worker, "_acquire_job", side_effect=acquire_results):
            worker.process_jobs()

        self.assertEqual(worker._pool.submit.call_count, 2)
        worker._pool.submit.assert_any_call(worker._execute_and_cleanup, 10)
        worker._pool.submit.assert_any_call(worker._execute_and_cleanup, 20)
        self.assertEqual(worker.active_job_ids, {10, 20})

    def test_execute_and_cleanup_removes_from_active(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        with worker._active_lock:
            worker.active_job_ids.add(42)

        with patch.object(worker, "execute_job"):
            worker._execute_and_cleanup(42)

        self.assertNotIn(42, worker.active_job_ids)

    def test_execute_and_cleanup_removes_on_exception(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        with worker._active_lock:
            worker.active_job_ids.add(42)

        with patch.object(worker, "execute_job", side_effect=RuntimeError("boom")):
            # Should not propagate — pool threads catch all exceptions
            worker._execute_and_cleanup(42)

        self.assertNotIn(42, worker.active_job_ids)

    def test_execute_and_cleanup_rolls_back_cursor(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        with worker._active_lock:
            worker.active_job_ids.add(42)

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(worker.db, "cursor", return_value=mock_cursor),
            patch.object(worker, "execute_job"),
        ):
            worker._execute_and_cleanup(42)

        mock_cursor.rollback.assert_called_once()

    def test_acquire_job_exception_does_not_corrupt_active_set(self):
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        original = set(worker.active_job_ids)

        with patch.object(
            worker, "acquire_job_lock", side_effect=RuntimeError("db down")
        ):
            with self.assertRaises(RuntimeError):
                worker._acquire_job()

        self.assertEqual(worker.active_job_ids, original)

    def test_update_heartbeats_uses_snapshot(self):
        """update_heartbeats takes a snapshot of active_job_ids under the lock."""
        worker = QueueWorker(self.env.cr.dbname, concurrency=2)
        with worker._active_lock:
            worker.active_job_ids.update({10, 20})

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        with patch.object(worker.db, "cursor", return_value=mock_cursor):
            worker.update_heartbeats()

        # Verify the SQL was called with a list snapshot (not the live set)
        args = mock_cursor.execute.call_args[0]
        self.assertIsInstance(args[1][0], list)
        self.assertEqual(sorted(args[1][0]), [10, 20])


@tagged("post_install", "-at_install")
class TestWorkerConcurrencyIntegration(TransactionCase):
    """Integration tests for concurrent job execution."""

    def setUp(self):
        super().setUp()
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            cr.commit()

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def test_concurrent_jobs_execute_in_parallel(self):
        """Two pool threads execute jobs simultaneously via a barrier."""
        token = uuid.uuid4().hex
        channel = f"parallel_{token}"

        with self._external_env() as (cr, env):
            env["queue.limit"].create({"name": channel, "limit": 10})
            for _ in range(2):
                env["queue.job"].enqueue(
                    model_name="res.users",
                    method_name="search",
                    record_ids=[],
                    args=[[("id", "=", 1)]],
                    kwargs={},
                    channel=channel,
                )
            cr.commit()

        barrier = threading.Barrier(2, timeout=10)
        parallel_achieved = threading.Event()

        def barrier_execute(cr, job_id):
            barrier.wait()
            parallel_achieved.set()

        stop_event = threading.Event()
        worker = QueueWorker(
            self.env.cr.dbname,
            concurrency=2,
            stop_event=stop_event,
            poll_timeout=1,
        )

        with patch.object(worker, "execute_job", side_effect=barrier_execute):
            thread = threading.Thread(target=worker.run, daemon=True)
            thread.start()

            parallel_achieved.wait(timeout=15)
            stop_event.set()
            thread.join(timeout=5)

        self.assertTrue(
            parallel_achieved.is_set(),
            "Two jobs should execute in parallel",
        )
