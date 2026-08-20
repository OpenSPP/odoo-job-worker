import importlib.util
import os
import threading
import time
import types
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
    def test_a_quarantine_with_no_recent_failures_expires_on_discovery(
        self, mock_has_module
    ):
        """The empty-history edge: nothing to prune, so nothing holds it."""
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
        )
        runner._quarantined_databases.add("db1")
        with patch.object(threading.Thread, "start"):
            runner.discover_databases()
        self.assertNotIn("db1", runner._quarantined_databases)

    def test_stopping_a_database_drops_its_quarantine(self):
        """A quarantine must not outlive the database it belonged to.

        `_fleet_is_degraded` reads the set directly, so an entry left behind
        for a database this process no longer serves withholds the heartbeat
        — and the healthcheck's staleness threshold is the same 60s as one
        discovery interval.
        """
        runner = QueueJobRunner(database_names=["db1"], use_advisory_lock=False)
        thread = MagicMock()
        thread.is_alive.return_value = False
        runner._worker_threads["db1"] = thread
        runner._per_database_stop_events["db1"] = threading.Event()
        runner._quarantined_databases.add("db1")

        runner._stop_worker_thread("db1")

        self.assertNotIn("db1", runner._quarantined_databases)
        self.assertFalse(runner._fleet_is_degraded())

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


class TestDatabaseErrorsNeverQuarantineTheDatabase(unittest.TestCase):
    """The incident's actual claim: a database error must not stop the database.

    Preprod 2026-07-30. `InFailedSqlTransaction` escaped every
    `OperationalError` guard, the worker thread ended, the supervisor restarted
    it, and after five deaths in 300s the whole database was quarantined — 47
    chunks stopped dead.

    The worker-side tests prove `run()` survives. These prove the thing that
    actually mattered, which is one level up: no failure is recorded, so the
    quarantine never arms. They are also why "return cleanly instead of
    raising" was not a fix — `_check_thread_health` cannot tell the difference,
    as the first test here shows.
    """

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            maximum_consecutive_failures=5,
            failure_window_seconds=300,
            registry_load_backoff_seconds=0.01,
            registry_load_backoff_cap_seconds=0.01,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def test_a_thread_that_keeps_ending_still_quarantines(self):
        """Why ending the thread cannot be the recovery.

        `_check_thread_health` treats *any* non-alive thread as a death: a
        clean `return` from `run()` is indistinguishable from a raise. This
        pins the mechanism the fix has to avoid rather than merely slow down.
        """
        runner = self._make_runner()
        with patch.object(threading.Thread, "start"):
            for _ in range(runner.maximum_consecutive_failures):
                dead = MagicMock()
                dead.is_alive.return_value = False
                runner._worker_threads["db1"] = dead
                runner._per_database_stop_events["db1"] = threading.Event()
                runner._check_thread_health()
        self.assertIn("db1", runner._quarantined_databases)

    def test_a_worker_recovering_in_place_is_never_recorded_as_a_death(self):
        """The fix, from the supervisor's side.

        The worker keeps its thread alive across database errors, so the health
        check sees nothing to restart, records no failure, and never arms the
        quarantine — however long the database stays broken.
        """
        runner = self._make_runner()
        alive = MagicMock()
        alive.is_alive.return_value = True
        runner._worker_threads["db1"] = alive
        runner._per_database_stop_events["db1"] = threading.Event()

        with patch.object(runner, "_start_worker_thread") as start:
            for _ in range(runner.maximum_consecutive_failures * 2):
                runner._check_thread_health()

        start.assert_not_called()
        self.assertEqual(runner._failure_timestamps, {})
        self.assertNotIn("db1", runner._quarantined_databases)

    def test_registry_load_retries_a_database_error_without_counting_a_death(self):
        """The half the incident actually went through.

        Its repeated crashes were raised from `env.ref()` during registry load,
        which runs before `worker.run()` is ever called — so no guard inside
        `run()` can see them. They landed in `_worker_thread_target`'s
        `except Exception`, which records a failure; five of those quarantined
        the database.
        """
        runner = self._make_runner()
        stop_event = threading.Event()
        attempts = []

        class _Registry:
            def __init__(self, db_name):
                attempts.append(db_name)
                if len(attempts) < 3:
                    raise _runner.psycopg2.Error("current transaction is aborted")

            def check_signaling(self):
                return None

        with patch.dict(
            "sys.modules",
            {"odoo.orm.registry": types.SimpleNamespace(Registry=_Registry)},
        ):
            loaded = runner._load_registry("db1", stop_event)

        self.assertTrue(loaded)
        self.assertEqual(len(attempts), 3)
        # The two failures were retried in place, not counted toward quarantine.
        self.assertEqual(runner._failure_timestamps, {})
        self.assertNotIn("db1", runner._quarantined_databases)
        # And the retry state is cleared once it succeeds, so a later health
        # check does not read it as still-degraded.
        self.assertNotIn("db1", runner._registry_load_retry_since)

    def test_registry_load_stops_promptly_when_asked(self):
        """An unreachable database must not delay shutdown."""
        runner = self._make_runner()
        stop_event = threading.Event()
        attempts = []

        class _Registry:
            def __init__(self, db_name):
                attempts.append(db_name)
                stop_event.set()
                raise _runner.psycopg2.Error("db unreachable")

            def check_signaling(self):  # pragma: no cover - never reached
                return None

        with patch.dict(
            "sys.modules",
            {"odoo.orm.registry": types.SimpleNamespace(Registry=_Registry)},
        ):
            loaded = runner._load_registry("db1", stop_event)

        self.assertFalse(loaded)
        self.assertEqual(len(attempts), 1)

    def test_a_non_database_registry_error_still_surfaces(self):
        """Only *database* errors retry in place.

        A genuine module import error is a real startup failure and must keep
        propagating to the caller, which records it — retrying that forever
        would replace a loud failure with a silent one.
        """
        runner = self._make_runner()

        class _Registry:
            def __init__(self, db_name):
                raise ImportError("no module named broken_addon")

            def check_signaling(self):  # pragma: no cover - never reached
                return None

        with patch.dict(
            "sys.modules",
            {"odoo.orm.registry": types.SimpleNamespace(Registry=_Registry)},
        ):
            with self.assertRaises(ImportError):
                runner._load_registry("db1", threading.Event())


class TestDegradedSignalSeesAnUnreachableDatabase(unittest.TestCase):
    """A wedged database must still turn the container unhealthy.

    Now that a database error records no failure, neither the quarantine set
    nor `_failure_timestamps` can see a database that is simply unreachable.
    Without a replacement signal the heartbeat file would keep being written
    and the container would report healthy while running no jobs at all —
    trading a loud crash loop for a silent dead worker.
    """

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            database_unhealthy_after_seconds=30,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def test_healthy_fleet_is_not_degraded(self):
        runner = self._make_runner()
        worker = MagicMock()
        worker.seconds_in_database_error_recovery.return_value = 0.0
        runner._worker_instances["db1"] = worker
        self.assertFalse(runner._fleet_is_degraded())

    def test_a_brief_recovery_is_not_degraded(self):
        """Otherwise a single transient blip would flap the container unhealthy."""
        runner = self._make_runner()
        worker = MagicMock()
        worker.seconds_in_database_error_recovery.return_value = 5.0
        runner._worker_instances["db1"] = worker
        self.assertFalse(runner._fleet_is_degraded())

    def test_a_long_recovery_is_degraded(self):
        runner = self._make_runner()
        worker = MagicMock()
        worker.seconds_in_database_error_recovery.return_value = 31.0
        runner._worker_instances["db1"] = worker
        self.assertTrue(runner._fleet_is_degraded())

    def test_a_database_stuck_loading_its_registry_is_degraded(self):
        """The worker instance does not exist yet, so this needs its own signal."""
        runner = self._make_runner()
        runner._registry_load_retry_since["db1"] = time.monotonic() - 31
        self.assertFalse(runner._worker_instances)
        self.assertTrue(runner._fleet_is_degraded())

    def test_a_degraded_fleet_stops_writing_the_heartbeat(self):
        """The signal is only worth anything if it reaches the healthcheck."""
        runner = self._make_runner()
        worker = MagicMock()
        worker.seconds_in_database_error_recovery.return_value = 31.0
        runner._worker_instances["db1"] = worker
        with patch.object(_runner, "write_heartbeat") as write:
            runner._update_heartbeat()
        write.assert_not_called()

    def test_threshold_zero_disables_the_signal(self):
        runner = self._make_runner(database_unhealthy_after_seconds=0)
        worker = MagicMock()
        worker.seconds_in_database_error_recovery.return_value = 9999.0
        runner._worker_instances["db1"] = worker
        self.assertFalse(runner._fleet_is_degraded())


class _FakeClock:
    """A monotonic clock the test advances by hand.

    ``QueueJobRunner`` reads only ``time.monotonic``, so substituting this for
    the module's ``time`` reference lets a test cover minutes of supervisor
    loop without sleeping — and without the timing flakiness that a real clock
    would bring to a window/interval resonance test.
    """

    def __init__(self, start=1000.0):
        self._now = start

    def monotonic(self):
        return self._now

    def advance(self, seconds):
        self._now += seconds


class TestQuarantineExpires(unittest.TestCase):
    """A quarantine must not outlive the fault that caused it.

    Dev payroll, 2026-08-11 (#30). Five registry-load crashes in 30 seconds
    quarantined the database; the supervisor then logged the same quarantine
    line once a minute for five hours and never retried, while 25 jobs sat
    `pending`. The data fault behind the crashes had cleared within the hour.

    The mechanism was a resonance between three settings that are independent
    everywhere else: `_check_thread_health` runs every ~10s, discovery cleared
    the quarantine every `discovery_interval_seconds` (60s), and
    `failure_window_seconds` (300s) is exactly `maximum_consecutive_failures`
    (5) discovery passes wide. So one corpse, re-counted once per pass, kept
    the window permanently full and the pruning that was meant to release the
    database could never drain.

    Expiry alone does not close that: with the corpse still in
    `_worker_threads`, each expiry hands `_check_thread_health` the same dead
    thread to count again, and five such stamps spaced one discovery apart
    rebuild the full window indefinitely. Counting a corpse once is the other
    half, and `test_a_corpse_kept_by_the_quarantine_is_counted_only_once`
    pins the geometry that needs it.
    """

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=["db1"],
            use_advisory_lock=False,
            maximum_consecutive_failures=5,
            failure_window_seconds=300,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def _quarantined_runner(self):
        runner = self._make_runner()
        dead = MagicMock()
        dead.is_alive.return_value = False
        runner._worker_threads["db1"] = dead
        runner._per_database_stop_events["db1"] = threading.Event()
        for _ in range(runner.maximum_consecutive_failures):
            runner._record_failure("db1")
        self.assertIn("db1", runner._quarantined_databases)
        return runner

    def test_one_dead_thread_is_not_recounted_on_every_discovery_pass(self):
        """The bug itself: a corpse already counted must not count again.

        Each pass re-counting the same non-alive thread is what refills the
        window. Nothing about the database got worse between these passes.
        """
        runner = self._quarantined_runner()
        recorded = len(runner._failure_timestamps["db1"])

        with (
            patch.object(_runner, "_database_has_module", return_value=True),
            patch.object(runner, "_start_worker_thread") as start,
        ):
            for _ in range(5):
                runner.discover_databases()
                runner._check_thread_health()

        self.assertEqual(len(runner._failure_timestamps["db1"]), recorded)
        start.assert_not_called()

    def test_a_quarantined_database_is_retried_once_its_failures_age_out(self):
        """The consequence: the database must get another chance.

        Ten simulated minutes at the real 60s discovery cadence — well past
        the 300s window — with no new deaths in between. Without expiry this
        never restarts, however long it runs, which is the outage.
        """
        clock = _FakeClock()
        runner = self._make_runner()
        dead = MagicMock()
        dead.is_alive.return_value = False
        runner._worker_threads["db1"] = dead
        runner._per_database_stop_events["db1"] = threading.Event()

        with (
            patch.object(_runner, "time", clock),
            patch.object(_runner, "_database_has_module", return_value=True),
            patch.object(runner, "_start_worker_thread") as start,
        ):
            # Arm the quarantine on the fake clock, so its timestamps sit on
            # the same timeline the pruning below compares against.
            for _ in range(runner.maximum_consecutive_failures):
                runner._record_failure("db1")
            self.assertIn("db1", runner._quarantined_databases)
            armed_at = clock.monotonic()

            for _ in range(10):
                clock.advance(60)
                runner.discover_databases()
                runner._check_thread_health()
                if start.called:
                    break

        self.assertTrue(
            start.called,
            "a quarantined database was never retried after its failure window elapsed",
        )
        self.assertGreaterEqual(
            clock.monotonic() - armed_at,
            runner.failure_window_seconds,
            "the retry came before the failures it was quarantined for aged out",
        )
        self.assertNotIn("db1", runner._quarantined_databases)

    def test_a_corpse_kept_by_the_quarantine_is_counted_only_once(self):
        """The geometry expiry alone does not escape.

        Five failure stamps one discovery pass apart — the spacing the
        expire-then-recount cycle produces by itself, and that any
        once-a-minute death pattern lands on. Each expiry hands the health
        check the same corpse; counting it again re-arms the quarantine
        before the restart, and the window is full once more. Four simulated
        hours of that is the #30 outage reached *through* the expiry fix, so
        the database has to come back here even though the arithmetic says
        the window is never empty.
        """
        clock = _FakeClock()
        runner = self._make_runner()
        dead = MagicMock()
        dead.is_alive.return_value = False
        runner._worker_threads["db1"] = dead
        runner._per_database_stop_events["db1"] = threading.Event()
        spacing = runner.failure_window_seconds / runner.maximum_consecutive_failures
        runner._failure_timestamps["db1"] = [
            clock.monotonic() - (spacing + 5) * age
            for age in range(runner.maximum_consecutive_failures - 1, -1, -1)
        ]
        runner._quarantined_databases.add("db1")

        with (
            patch.object(_runner, "time", clock),
            patch.object(_runner, "_database_has_module", return_value=True),
            patch.object(runner, "_start_worker_thread") as start,
        ):
            last_discovery = clock.monotonic()
            deadline = clock.monotonic() + 4 * 3600
            while clock.monotonic() < deadline and not start.called:
                clock.advance(10)
                if clock.monotonic() - last_discovery >= 60:
                    runner.discover_databases()
                    last_discovery = clock.monotonic()
                runner._check_thread_health()

        self.assertTrue(
            start.called,
            "one corpse, re-counted once per discovery pass, kept the failure "
            "window full for four simulated hours and the database was never "
            "retried",
        )

    def test_a_crash_is_counted_as_one_failure_not_two(self):
        """A crashing worker records its own death; the corpse is not re-counted.

        ``_worker_thread_target`` stamps the failure from inside the thread,
        and ``_check_thread_health`` then finds the same thread not alive.
        Both counting it is what made ``maximum_consecutive_failures`` fire at
        half its stated number of real crashes — and, once a quarantine can
        expire, is the extra stamp that keeps the window full.
        """
        runner = self._make_runner()
        registry_class = MagicMock(side_effect=RuntimeError("broken module"))
        composite = threading.Event()

        with (
            patch.dict(
                "sys.modules",
                {
                    "odoo.orm": MagicMock(),
                    "odoo.orm.registry": MagicMock(Registry=registry_class),
                },
            ),
            patch.object(runner, "_start_worker_thread"),
        ):
            # A real thread, because the corpse the health check inspects has
            # to be the same object the crashing worker claimed.
            thread = threading.Thread(
                target=runner._worker_thread_target, args=("db1", composite)
            )
            runner._worker_threads["db1"] = thread
            runner._per_database_stop_events["db1"] = threading.Event()
            thread.start()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive(), "the worker thread never ended")
            runner._check_thread_health()

        self.assertEqual(len(runner._failure_timestamps["db1"]), 1)

    def test_a_still_failing_database_stays_quarantined(self):
        """Expiry must not become "retry forever, immediately".

        A database that keeps killing its worker has to settle back into
        quarantine rather than spin, so the retries stay rare enough to be a
        recovery attempt rather than a crash loop. Driven on the fake clock
        because the property is about *when* the retries happen: releasing
        every quarantine unconditionally passes any test whose failure stamps
        are too young to age out.
        """
        clock = _FakeClock()
        runner = self._make_runner()
        restarts = []

        def die_immediately(db_name):
            """Restart a worker that crashes before the next tick."""
            thread = MagicMock()
            thread.is_alive.return_value = False
            runner._worker_threads[db_name] = thread
            runner._per_database_stop_events[db_name] = threading.Event()
            # The worker claims and records its own death, as the
            # ``except Exception`` in ``_worker_thread_target`` does.
            runner._deaths_counted.add(thread)
            runner._record_failure(db_name)
            restarts.append(clock.monotonic())

        with (
            patch.object(_runner, "time", clock),
            patch.object(_runner, "_database_has_module", return_value=True),
            patch.object(runner, "_start_worker_thread", die_immediately),
        ):
            die_immediately("db1")
            while "db1" not in runner._quarantined_databases:
                clock.advance(10)
                runner._check_thread_health()
            restarts_when_armed = len(restarts)
            # The quarantine lifts one window after the *oldest* stamp in the
            # burst, not one window after it armed: expiry watches the pruned
            # count, and that drops below the threshold as soon as the first
            # stamp ages out.
            held_until = (
                min(runner._failure_timestamps["db1"]) + runner.failure_window_seconds
            )

            # Held for as long as the failures that armed it are still recent.
            while clock.monotonic() < held_until:
                clock.advance(10)
                runner.discover_databases()
                runner._check_thread_health()
            self.assertEqual(
                len(restarts),
                restarts_when_armed,
                "a quarantined database was retried while its failures were "
                "still recent",
            )

            # An hour of a database that never recovers.
            last_discovery = clock.monotonic()
            deadline = clock.monotonic() + 3600
            while clock.monotonic() < deadline:
                clock.advance(10)
                if clock.monotonic() - last_discovery >= 60:
                    runner.discover_databases()
                    last_discovery = clock.monotonic()
                runner._check_thread_health()

        self.assertIn("db1", runner._quarantined_databases)
        # It does keep trying — a quarantine that never lifts is #30 again.
        self.assertGreater(len(restarts), restarts_when_armed)
        # ...but in bursts of at most `maximum_consecutive_failures` per
        # window, not once per 10s tick (which would be 360 in the hour).
        windows = 3600 / runner.failure_window_seconds
        self.assertLessEqual(
            len(restarts) - restarts_when_armed,
            windows * runner.maximum_consecutive_failures * 2,
            "expiry turned a stuck queue into a restart spin",
        )
