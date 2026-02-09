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
