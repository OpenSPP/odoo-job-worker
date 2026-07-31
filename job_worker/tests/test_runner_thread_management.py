import importlib.util
import os
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

# Load runner module with stubbed Odoo dependencies.
_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_spec = importlib.util.spec_from_file_location("_runner_test_helpers", _helpers_path)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()

QueueJobRunner = _runner.QueueJobRunner


class TestThreadHealth(unittest.TestCase):
    """Tests for _check_thread_health and failure tracking."""

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            maximum_consecutive_failures=3,
            failure_window_seconds=60,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def test_healthy_thread_not_restarted(self):
        runner = self._make_runner()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        with patch.object(runner, "_start_worker_thread") as mock_start:
            runner._check_thread_health()
        mock_start.assert_not_called()

    def test_dead_thread_is_restarted(self):
        runner = self._make_runner()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        with patch.object(threading.Thread, "start"):
            runner._check_thread_health()
        self.assertIn("db1", runner._worker_threads)
        # The thread object should be different from the original mock
        self.assertIsNot(runner._worker_threads["db1"], mock_thread)

    def test_dead_thread_failure_recorded(self):
        runner = self._make_runner()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        with patch.object(threading.Thread, "start"):
            runner._check_thread_health()
        self.assertIn("db1", runner._failure_timestamps)
        self.assertEqual(len(runner._failure_timestamps["db1"]), 1)

    def test_quarantined_database_not_restarted(self):
        runner = self._make_runner()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()
        runner._quarantined_databases.add("db1")

        with patch.object(runner, "_start_worker_thread") as mock_start:
            runner._check_thread_health()
        mock_start.assert_not_called()

    def test_quarantine_after_consecutive_failures(self):
        runner = self._make_runner(maximum_consecutive_failures=3)
        for _ in range(3):
            runner._record_failure("db1")
        self.assertIn("db1", runner._quarantined_databases)

    def test_no_quarantine_below_threshold(self):
        runner = self._make_runner(maximum_consecutive_failures=3)
        for _ in range(2):
            runner._record_failure("db1")
        self.assertNotIn("db1", runner._quarantined_databases)

    def test_old_failures_outside_window_pruned(self):
        runner = self._make_runner(failure_window_seconds=60)
        # Manually add old timestamps outside the window
        old_time = time.monotonic() - 120
        runner._failure_timestamps["db1"] = [old_time, old_time + 1]
        runner._record_failure("db1")
        # Old timestamps should be pruned, only 1 recent failure remains
        self.assertEqual(len(runner._failure_timestamps["db1"]), 1)


class TestDiscoveryAndRemoval(unittest.TestCase):
    """Tests for hot addition and removal of databases."""

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_rediscovery_clears_quarantine(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
        )
        runner._quarantined_databases.add("db1")
        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertNotIn("db1", runner._quarantined_databases)

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_rediscovery_adds_new_database(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1", "db2"],
            use_advisory_lock=False,
        )
        # Simulate db1 already running
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertIn("db2", runner._worker_threads)
        # db1's thread should be unchanged
        self.assertIs(runner._worker_threads["db1"], mock_thread)

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_rediscovery_removes_gone_database(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
        )
        # Simulate db2 currently running but not in database_names
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db2"] = mock_thread
        runner._per_database_stop_events["db2"] = threading.Event()

        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertNotIn("db2", runner._worker_threads)

    def test_concurrent_stop_during_health_check(self):
        runner = QueueJobRunner(
            database_names=[],
            use_advisory_lock=False,
        )
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()
        runner.stop_event.set()

        with patch.object(runner, "_start_worker_thread") as mock_start:
            runner._check_thread_health()
        mock_start.assert_not_called()

    def test_stop_worker_thread_logs_warning_on_join_timeout(self):
        runner = QueueJobRunner(
            database_names=[],
            use_advisory_lock=False,
            join_timeout_seconds=0.01,
        )
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True  # Simulates thread not stopping
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        with self.assertLogs(_runner._logger, level="WARNING") as log:
            runner._stop_worker_thread("db1")
        self.assertTrue(
            any("did not stop" in msg for msg in log.output),
            f"Expected 'did not stop' warning in: {log.output}",
        )

    @patch.object(_runner, "_get_database_names", return_value=[])
    def test_runner_continues_with_zero_databases(self, mock_get_names):
        runner = QueueJobRunner(use_advisory_lock=False)
        runner.discover_databases()
        # No threads started, no crash
        self.assertEqual(runner._worker_threads, {})

    @patch.object(_runner, "_database_has_module")
    def test_discovery_failure_for_one_database_does_not_block_others(
        self, mock_has_module
    ):
        def has_module_side_effect(db_name, **kwargs):
            if db_name == "bad_db":
                raise Exception("connection refused")
            return True

        mock_has_module.side_effect = has_module_side_effect
        runner = QueueJobRunner(
            database_names=["bad_db", "good_db"],
            use_advisory_lock=False,
        )
        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertNotIn("bad_db", runner._worker_threads)
        self.assertIn("good_db", runner._worker_threads)


class TestWorkerProgressWatchdog(unittest.TestCase):
    """The supervisor must recycle a hung-but-alive worker.

    _check_thread_health only restarts *dead* threads. A worker blocked in a
    DB call keeps is_alive()==True but stops advancing last_progress — the
    zombie-worker failure mode. _check_worker_progress must catch that and
    escalate to a process exit (the only reliable recovery for a hung
    thread), which _terminate_stalled_worker isolates for testability.
    """

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            worker_stall_timeout_seconds=120,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def _alive_thread(self):
        thread = MagicMock()
        thread.is_alive.return_value = True
        return thread

    def _register(self, runner, db_name, last_progress, thread):
        worker = MagicMock()
        worker.last_progress = last_progress
        runner._worker_instances[db_name] = worker
        runner._worker_threads[db_name] = thread

    def _register_worker(self, runner, db_name, worker_uuid):
        """Register a live worker whose in-flight rows are stampable."""
        worker = MagicMock()
        worker.worker_uuid = worker_uuid
        runner._worker_instances[db_name] = worker
        runner._worker_threads[db_name] = self._alive_thread()
        return worker

    def _run_terminate(self, runner, db_name, age):
        """Drive the watchdog kill with the database layer stubbed out.

        Returns the single cursor every stamped database shares, so the caller
        reads all of the UPDATEs off one call list.
        """
        cursor = MagicMock()
        cursor.rowcount = 2
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with (
            patch.object(_runner.os, "_exit"),
            patch.object(_runner.logging, "shutdown"),
            patch.object(_runner.psycopg2, "connect", return_value=conn, create=True),
            patch.object(
                _runner.odoo.sql_db,
                "connection_info_for",
                # A fresh dict per call: _stamp_one_db mutates it with
                # connect_timeout, and a shared one would hide a stamp that
                # relied on another database's leftovers.
                side_effect=lambda name: (name, {}),
                create=True,
            ),
        ):
            runner._terminate_stalled_worker(db_name, age)
        return cursor

    def _update_calls(self, cursor):
        return [
            call
            for call in cursor.execute.call_args_list
            if "UPDATE queue_job" in call.args[0]
        ]

    def test_stalled_worker_is_terminated(self):
        runner = self._make_runner()
        self._register(runner, "db1", time.monotonic() - 999, self._alive_thread())
        with patch.object(runner, "_terminate_stalled_worker") as term:
            runner._check_worker_progress()
        term.assert_called_once()
        self.assertEqual(term.call_args[0][0], "db1")

    def test_progressing_worker_is_left_alone(self):
        runner = self._make_runner()
        self._register(runner, "db1", time.monotonic(), self._alive_thread())
        with patch.object(runner, "_terminate_stalled_worker") as term:
            runner._check_worker_progress()
        term.assert_not_called()

    def test_dead_thread_is_not_the_watchdogs_job(self):
        # A dead thread is _check_thread_health's responsibility; the progress
        # watchdog only fires for threads that are alive but not progressing.
        runner = self._make_runner()
        dead = MagicMock()
        dead.is_alive.return_value = False
        self._register(runner, "db1", time.monotonic() - 999, dead)
        with patch.object(runner, "_terminate_stalled_worker") as term:
            runner._check_worker_progress()
        term.assert_not_called()

    def test_watchdog_disabled_when_timeout_zero(self):
        runner = self._make_runner(worker_stall_timeout_seconds=0)
        self._register(runner, "db1", time.monotonic() - 999, self._alive_thread())
        with patch.object(runner, "_terminate_stalled_worker") as term:
            runner._check_worker_progress()
        term.assert_not_called()

    def test_terminate_exits_process(self):
        runner = self._make_runner()
        # Patch logging.shutdown too — the real one would tear down logging for
        # the whole test process (os._exit is patched, so it does not exit).
        with (
            patch.object(_runner.os, "_exit") as mock_exit,
            patch.object(_runner.logging, "shutdown") as mock_shutdown,
        ):
            runner._terminate_stalled_worker("db1", 999)
        mock_exit.assert_called_once_with(1)
        mock_shutdown.assert_called_once()

    def test_terminate_stamps_inflight_jobs_as_stalled_not_oom(self):
        """A watchdog kill must be distinguishable from an OOM kill.

        Both leave the job to be reclaimed as WorkerDiedJobError with no other
        diagnosis, yet they have opposite remedies (find the blocked query vs.
        lower the memory ceiling). Naming the stall on the row is what stops the
        next investigation going after limit_memory_hard for hours.
        """
        runner = self._make_runner()
        self._register_worker(runner, "db1", "worker-uuid-1")
        cursor = self._run_terminate(runner, "db1", 130)

        updates = self._update_calls(cursor)
        self.assertEqual(len(updates), 1)
        sql, params = updates[0].args
        # Scoped to this worker's own in-flight rows.
        self.assertIn("state = 'started'", sql)
        self.assertIn("worker_id = %s", sql)
        # The reason is appended rather than overwritten, so it is passed twice
        # (the NULL/empty branch and the concatenating branch of the CASE) and
        # the worker id is the *third* parameter, not the second. Asserting the
        # position pins that: a silent reordering would send a worker uuid into
        # exc_info and the scoping predicate a sentence of prose.
        self.assertIn("exc_info || ", sql)
        self.assertEqual(params[2], "worker-uuid-1")
        self.assertEqual(params[0], params[1])
        self.assertIn("WorkerStalledError", params[0])
        self.assertIn("NOT an OOM", params[0])

    def test_terminate_stamps_every_database_this_process_serves(self):
        """os._exit orphans in-flight jobs on every database, not just the stalled one.

        A worker on a healthy database loses its jobs to the same kill, and
        without a stamp of their own those rows stay OOM-ambiguous — the exact
        confusion this feature exists to remove. They get a reason naming the
        database that actually stalled, so the diagnosis points at the culprit
        rather than at them.
        """
        runner = self._make_runner()
        self._register_worker(runner, "db1", "worker-uuid-1")
        self._register_worker(runner, "db2", "worker-uuid-2")

        cursor = self._run_terminate(runner, "db1", 130)

        by_worker = {
            call.args[1][2]: call.args[1][0] for call in self._update_calls(cursor)
        }
        self.assertEqual(set(by_worker), {"worker-uuid-1", "worker-uuid-2"})

        # The stalled database is named as the culprit.
        self.assertIn("made no progress for 130s", by_worker["worker-uuid-1"])

        # The bystander is told it was collateral, and which database to look at.
        collateral = by_worker["worker-uuid-2"]
        self.assertIn("orphaned because the worker for database 'db1'", collateral)
        self.assertIn("was not necessarily", collateral)
        self.assertIn("NOT an OOM", collateral)

    def test_terminate_still_exits_when_stamping_fails(self):
        """Diagnostics must never keep a hung process alive.

        A stalled worker often means a wedged database, so the stamp is the most
        likely thing to fail — and it is the least important thing to succeed.
        """
        runner = self._make_runner()
        worker = MagicMock()
        worker.worker_uuid = "worker-uuid-2"
        runner._worker_instances["db1"] = worker
        runner._worker_threads["db1"] = self._alive_thread()

        with (
            patch.object(_runner.os, "_exit") as mock_exit,
            patch.object(_runner.logging, "shutdown"),
            patch.object(
                _runner.odoo.sql_db,
                "connection_info_for",
                return_value=("db1", {}),
                create=True,
            ),
            patch.object(
                _runner.psycopg2,
                "connect",
                side_effect=OSError("db unreachable"),
                create=True,
            ) as mock_connect,
        ):
            runner._terminate_stalled_worker("db1", 130)

        # The stamp really was attempted and really did fail — without this the
        # test would pass without ever exercising the failure path.
        mock_connect.assert_called_once()
        mock_exit.assert_called_once_with(1)
