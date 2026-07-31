import importlib.util
import os
import signal
import threading
import unittest
from unittest.mock import MagicMock, patch

# Load runner module with stubbed Odoo dependencies.
_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_spec = importlib.util.spec_from_file_location("_runner_test_helpers", _helpers_path)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()

QueueJobRunner = _runner.QueueJobRunner


class TestRunnerLifecycle(unittest.TestCase):
    """Tests for QueueJobRunner init, run, stop, and cleanup."""

    def test_stops_immediately_when_stop_event_preset(self):
        runner = QueueJobRunner(database_names=["db1"])
        runner.stop_event.set()
        runner.run()
        # Should exit immediately without spawning threads
        self.assertEqual(runner._worker_threads, {})

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_discovers_and_starts_worker_threads(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1", "db2"],
            use_advisory_lock=False,
        )

        def stop_after_discovery():
            if runner._worker_threads:
                runner.stop_event.set()

        with patch.object(runner, "_worker_thread_target"):
            runner.discover_databases()
            self.assertIn("db1", runner._worker_threads)
            self.assertIn("db2", runner._worker_threads)
            runner.stop_event.set()

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_passes_composite_stop_event_to_workers(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
        )
        with patch.object(threading.Thread, "start"):
            runner.discover_databases()

        # The per-database stop event should exist
        self.assertIn("db1", runner._per_database_stop_events)

    def test_stop_sets_global_event(self):
        runner = QueueJobRunner(database_names=[])
        self.assertFalse(runner.stop_event.is_set())
        runner.stop()
        self.assertTrue(runner.stop_event.is_set())

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_cleanup_joins_all_threads_with_timeout(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
            join_timeout_seconds=5,
        )
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        runner._cleanup()
        mock_thread.join.assert_called_once_with(timeout=5)

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_cleanup_closes_advisory_lock_connections(self, mock_has_module):
        runner = QueueJobRunner(database_names=["db1"])
        mock_conn = MagicMock()
        runner._advisory_lock_connections["db1"] = mock_conn
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        runner._worker_threads["db1"] = mock_thread
        runner._per_database_stop_events["db1"] = threading.Event()

        runner._cleanup()
        mock_conn.close.assert_called_once()

    def test_signal_handler_calls_stop(self):
        runner = QueueJobRunner(database_names=[])
        runner._handle_signal(signal.SIGTERM, None)
        self.assertTrue(runner.stop_event.is_set())

    def test_signal_handler_skipped_on_non_main_thread(self):
        runner = QueueJobRunner(database_names=[])
        result = {}

        def setup_in_thread():
            runner._setup_signal_handlers()
            result["ok"] = True

        thread = threading.Thread(target=setup_in_thread)
        thread.start()
        thread.join(timeout=2)
        # Should not crash, just log a warning
        self.assertTrue(result.get("ok", False))

    @patch.object(_runner, "_database_has_module", return_value=False)
    def test_skips_database_without_module(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
        )
        runner.discover_databases()
        self.assertEqual(runner._worker_threads, {})

    @patch.object(_runner, "_try_advisory_lock", return_value=None)
    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_skips_database_when_advisory_lock_fails(self, mock_has_module, mock_lock):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=True,
        )
        runner.discover_databases()
        self.assertEqual(runner._worker_threads, {})

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_starts_all_when_advisory_lock_disabled(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1", "db2"],
            use_advisory_lock=False,
        )
        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertIn("db1", runner._worker_threads)
        self.assertIn("db2", runner._worker_threads)

    def test_from_environ_or_config_reads_config(self):
        with patch.object(_runner, "odoo") as mock_odoo:
            mock_odoo.tools.config = {"db_name": "mydb"}
            runner = QueueJobRunner.from_environ_or_config()
            self.assertEqual(runner.database_names, ["mydb"])

    def test_from_environ_or_config_reads_concurrency_env(self):
        with (
            patch.object(_runner, "odoo") as mock_odoo,
            patch.dict(os.environ, {"QUEUE_JOB_CONCURRENCY": "4"}),
        ):
            mock_odoo.tools.config = {"db_name": ""}
            runner = QueueJobRunner.from_environ_or_config()
            self.assertEqual(runner.worker_keyword_arguments["concurrency"], 4)

    def test_from_environ_or_config_defaults_concurrency(self):
        with (
            patch.object(_runner, "odoo") as mock_odoo,
            patch.dict(os.environ, {}, clear=False),
        ):
            mock_odoo.tools.config = {"db_name": ""}
            os.environ.pop("QUEUE_JOB_CONCURRENCY", None)
            runner = QueueJobRunner.from_environ_or_config()
            self.assertEqual(runner.worker_keyword_arguments["concurrency"], 2)

    @patch.object(_runner, "_database_has_module", return_value=True)
    def test_worker_keyword_arguments_forwarded(self, mock_has_module):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
            worker_keyword_arguments={"poll_timeout": 10},
        )
        self.assertEqual(runner.worker_keyword_arguments, {"poll_timeout": 10})

    def test_worker_thread_target_preloads_registry(self):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
            worker_keyword_arguments={"concurrency": 2},
        )
        mock_registry_class = MagicMock()
        mock_registry = mock_registry_class.return_value
        mock_worker_class = MagicMock()
        mock_worker_instance = mock_worker_class.return_value
        # A real Event, not a MagicMock: the registry load is now a retry loop
        # guarded by `while not stop_event.is_set()`, and a MagicMock's is_set()
        # returns a truthy mock, so the loop would exit before loading anything.
        composite = threading.Event()

        with (
            patch.dict(
                "sys.modules",
                {
                    "odoo.orm": MagicMock(),
                    "odoo.orm.registry": MagicMock(Registry=mock_registry_class),
                },
            ),
            patch.object(_runner, "QueueWorker", mock_worker_class),
        ):
            runner._worker_thread_target("db1", composite)

        mock_registry_class.assert_called_once_with("db1")
        mock_registry.check_signaling.assert_called_once()
        mock_worker_class.assert_called_once_with(
            "db1",
            stop_event=composite,
            concurrency=2,
        )
        mock_worker_instance.run.assert_called_once()
