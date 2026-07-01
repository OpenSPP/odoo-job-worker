"""Tests for the heartbeat liveness mechanism.

These are pure-Python / unittest tests that do not require Odoo.  They
exercise:

* ``job_worker.cli.heartbeat`` — the standard-library-only helper used by
  both the runner (writer) and the standalone healthcheck (reader).
* ``QueueJobRunner._update_heartbeat`` — the supervisor-loop integration
  that refreshes the heartbeat only while the worker fleet is healthy.

They are run via ``run_runner_tests.py``, not the Odoo test runner, so the
stubbed-Odoo import machinery in ``_runner_test_helpers`` does not leak into
real Odoo test sessions.
"""

import importlib.util
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

# Load the heartbeat helper directly from its file so the pure-helper tests
# do not depend on Odoo or the runner's stubs.
_HEARTBEAT_PATH = os.path.join(os.path.dirname(__file__), "..", "cli", "heartbeat.py")
_hb_spec = importlib.util.spec_from_file_location(
    "job_worker_heartbeat_under_test", _HEARTBEAT_PATH
)
heartbeat = importlib.util.module_from_spec(_hb_spec)
_hb_spec.loader.exec_module(heartbeat)

# Load the runner module with stubbed Odoo dependencies for the integration
# tests of _update_heartbeat.
_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_helpers_spec = importlib.util.spec_from_file_location(
    "_runner_test_helpers", _helpers_path
)
_helpers = importlib.util.module_from_spec(_helpers_spec)
_helpers_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()
QueueJobRunner = _runner.QueueJobRunner


class TestHeartbeatFile(unittest.TestCase):
    """Tests for the write/read/freshness helpers in heartbeat.py."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._tmpdir.name, "heartbeat")

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_write_then_read_roundtrip(self):
        heartbeat.write_heartbeat(self.path, now=1234567.5)
        self.assertEqual(heartbeat.read_heartbeat(self.path), 1234567.5)

    def test_is_fresh_true_for_recent_write(self):
        heartbeat.write_heartbeat(self.path)
        self.assertTrue(heartbeat.is_fresh(self.path, max_age=60))

    def test_is_fresh_false_for_old_write(self):
        heartbeat.write_heartbeat(self.path, now=time.time() - 1000)
        self.assertFalse(heartbeat.is_fresh(self.path, max_age=60))

    def test_is_fresh_false_for_missing_file(self):
        missing = os.path.join(self._tmpdir.name, "does-not-exist")
        self.assertFalse(heartbeat.is_fresh(missing, max_age=60))

    def test_read_returns_none_for_missing_file(self):
        missing = os.path.join(self._tmpdir.name, "does-not-exist")
        self.assertIsNone(heartbeat.read_heartbeat(missing))

    def test_read_returns_none_for_empty_file(self):
        with open(self.path, "w"):
            pass
        self.assertIsNone(heartbeat.read_heartbeat(self.path))

    def test_read_returns_none_for_corrupt_file(self):
        with open(self.path, "w") as handle:
            handle.write("not-a-number")
        self.assertIsNone(heartbeat.read_heartbeat(self.path))

    def test_is_fresh_false_for_corrupt_file(self):
        with open(self.path, "w") as handle:
            handle.write("garbage")
        self.assertFalse(heartbeat.is_fresh(self.path, max_age=60))

    def test_write_is_atomic_no_leftover_tmp(self):
        heartbeat.write_heartbeat(self.path)
        leftovers = [
            name for name in os.listdir(self._tmpdir.name) if name.endswith(".tmp")
        ]
        self.assertEqual(leftovers, [])

    def test_write_overwrites_previous_value(self):
        heartbeat.write_heartbeat(self.path, now=100.0)
        heartbeat.write_heartbeat(self.path, now=200.0)
        self.assertEqual(heartbeat.read_heartbeat(self.path), 200.0)


class TestHeartbeatEnvResolution(unittest.TestCase):
    """Tests for environment-driven path and max-age resolution."""

    def test_path_from_env(self):
        with patch.dict(os.environ, {heartbeat.ENV_HEARTBEAT_FILE: "/custom/hb"}):
            self.assertEqual(heartbeat.heartbeat_file_path(), "/custom/hb")

    def test_path_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(heartbeat.ENV_HEARTBEAT_FILE, None)
            self.assertEqual(
                heartbeat.heartbeat_file_path(), heartbeat.DEFAULT_HEARTBEAT_FILE
            )

    def test_max_age_from_env(self):
        with patch.dict(os.environ, {heartbeat.ENV_MAX_AGE_SECONDS: "90"}):
            self.assertEqual(heartbeat.max_age_seconds(), 90)

    def test_max_age_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(heartbeat.ENV_MAX_AGE_SECONDS, None)
            self.assertEqual(
                heartbeat.max_age_seconds(), heartbeat.DEFAULT_MAX_AGE_SECONDS
            )

    def test_max_age_default_when_invalid(self):
        with patch.dict(os.environ, {heartbeat.ENV_MAX_AGE_SECONDS: "abc"}):
            self.assertEqual(
                heartbeat.max_age_seconds(), heartbeat.DEFAULT_MAX_AGE_SECONDS
            )

    def test_max_age_default_when_non_positive(self):
        with patch.dict(os.environ, {heartbeat.ENV_MAX_AGE_SECONDS: "0"}):
            self.assertEqual(
                heartbeat.max_age_seconds(), heartbeat.DEFAULT_MAX_AGE_SECONDS
            )


class TestRunnerHeartbeat(unittest.TestCase):
    """Tests for QueueJobRunner._update_heartbeat gating."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._tmpdir.name, "heartbeat")

    def tearDown(self):
        self._tmpdir.cleanup()

    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            maximum_consecutive_failures=3,
            failure_window_seconds=300,
            heartbeat_file=self.path,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def test_init_uses_provided_heartbeat_file(self):
        runner = self._make_runner()
        self.assertEqual(runner._heartbeat_file, self.path)

    def test_update_writes_heartbeat_when_healthy(self):
        runner = self._make_runner()
        runner._update_heartbeat()
        self.assertTrue(os.path.exists(self.path))
        self.assertTrue(heartbeat.is_fresh(self.path, max_age=60))

    def test_update_skips_heartbeat_when_quarantined(self):
        runner = self._make_runner()
        runner._quarantined_databases.add("db1")
        runner._update_heartbeat()
        self.assertFalse(os.path.exists(self.path))

    def test_update_skips_heartbeat_when_failures_exceed_threshold(self):
        # Failure history survives the quarantine reset that rediscovery
        # performs, so a database past the threshold keeps the heartbeat
        # stale even when _quarantined_databases is momentarily empty.
        runner = self._make_runner(maximum_consecutive_failures=3)
        now = time.monotonic()
        runner._failure_timestamps["db1"] = [now, now, now]
        self.assertEqual(runner._quarantined_databases, set())
        runner._update_heartbeat()
        self.assertFalse(os.path.exists(self.path))

    def test_update_writes_when_failures_below_threshold(self):
        runner = self._make_runner(maximum_consecutive_failures=3)
        now = time.monotonic()
        runner._failure_timestamps["db1"] = [now, now]
        runner._update_heartbeat()
        self.assertTrue(os.path.exists(self.path))

    def test_update_writes_when_failures_aged_out_of_window(self):
        runner = self._make_runner(
            maximum_consecutive_failures=3, failure_window_seconds=300
        )
        old = time.monotonic() - 1000
        runner._failure_timestamps["db1"] = [old, old, old]
        runner._update_heartbeat()
        self.assertTrue(os.path.exists(self.path))

    def test_update_swallows_write_errors(self):
        # A heartbeat write failure must never crash the supervisor.
        runner = self._make_runner(
            heartbeat_file=os.path.join(self._tmpdir.name, "missing-dir", "hb")
        )
        with self.assertLogs(_runner._logger, level="WARNING"):
            runner._update_heartbeat()  # should not raise
        self.assertFalse(os.path.exists(runner._heartbeat_file))

    def test_update_swallows_degraded_check_errors(self):
        # _fleet_is_degraded reads state that worker threads mutate
        # concurrently; an error there must never escape into the
        # supervisor loop run() (which has no surrounding try/except).
        runner = self._make_runner()
        with patch.object(
            runner, "_fleet_is_degraded", side_effect=RuntimeError("boom")
        ):
            with self.assertLogs(_runner._logger, level="WARNING"):
                runner._update_heartbeat()  # should not raise
        self.assertFalse(os.path.exists(self.path))

    def test_run_loop_writes_heartbeat(self):
        runner = self._make_runner(discovery_interval_seconds=0)
        with patch.object(runner, "_setup_signal_handlers"):
            thread = threading.Thread(target=runner.run)
            thread.start()
            deadline = time.time() + 5
            while time.time() < deadline and not os.path.exists(self.path):
                time.sleep(0.02)
            runner.stop()
            thread.join(timeout=5)
        self.assertTrue(os.path.exists(self.path))
        self.assertTrue(heartbeat.is_fresh(self.path, max_age=60))


class TestHealthcheckScript(unittest.TestCase):
    """End-to-end tests of the standalone job_worker_healthcheck.py entry point."""

    _SCRIPT = os.path.join(
        os.path.dirname(__file__), "..", "..", "job_worker_healthcheck.py"
    )

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._tmpdir.name, "heartbeat")

    def tearDown(self):
        self._tmpdir.cleanup()

    def _run(self, max_age=None):
        env = dict(os.environ, JOB_WORKER_HEARTBEAT_FILE=self.path)
        if max_age is not None:
            env["JOB_WORKER_HEARTBEAT_MAX_AGE"] = str(max_age)
        return subprocess.run(
            [sys.executable, self._SCRIPT], env=env, capture_output=True
        )

    def test_exit_zero_when_heartbeat_fresh(self):
        heartbeat.write_heartbeat(self.path)
        self.assertEqual(self._run().returncode, 0)

    def test_exit_one_when_heartbeat_missing(self):
        self.assertEqual(self._run().returncode, 1)

    def test_exit_one_when_heartbeat_stale(self):
        heartbeat.write_heartbeat(self.path, now=time.time() - 1000)
        self.assertEqual(self._run().returncode, 1)

    def test_max_age_env_is_honored(self):
        heartbeat.write_heartbeat(self.path, now=time.time() - 30)
        # 30s-old heartbeat: stale under a 10s threshold, fresh under 120s.
        self.assertEqual(self._run(max_age=10).returncode, 1)
        self.assertEqual(self._run(max_age=120).returncode, 0)
