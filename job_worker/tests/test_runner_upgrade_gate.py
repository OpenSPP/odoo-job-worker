"""How the runner uses the upgrade gate and the process guard.

Pure-Python / unittest tests, run via ``run_runner_tests.py``. The gate's own
decision logic is covered by ``test_upgrade_gate.py``; these pin the runner's
side of it:

* the first registry load waits for the gate, and never happens while it is
  closed;
* one gate per database is shared with that database's ``QueueWorker``;
* a pause that has outlasted its threshold stales the heartbeat;
* ``run()`` installs the process guard;
* the gate's settings are read from the environment and validated.
"""

import importlib.util
import os
import threading
import types
import unittest
from unittest.mock import MagicMock, patch

_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_spec = importlib.util.spec_from_file_location("_runner_test_helpers", _helpers_path)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()

QueueJobRunner = _runner.QueueJobRunner

GATE_ENV_VARS = (
    "JOB_WORKER_UPGRADE_GATE",
    "JOB_WORKER_UPGRADE_GATE_VERSION_CHECK",
    "JOB_WORKER_UPGRADE_GATE_INTERVAL",
    "JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER",
)


class _FakeGate:
    """Answers ``is_open`` from a script; the last answer repeats."""

    interval_seconds = 0.001

    def __init__(self, answers, overdue=False, on_check=None):
        self._answers = list(answers)
        self.calls = 0
        self.overdue = overdue
        self._on_check = on_check

    def is_open(self):
        self.calls += 1
        if self._on_check is not None:
            self._on_check(self.calls)
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def is_overdue(self):
        return self.overdue


def _registry_module(loads):
    class _Registry:
        def __init__(self, db_name):
            loads.append(db_name)

        def check_signaling(self):
            return None

    return {"odoo.orm.registry": types.SimpleNamespace(Registry=_Registry)}


class TestRegistryLoadWaitsForTheGate(unittest.TestCase):
    def _make_runner(self, **kwargs):
        defaults = dict(
            database_names=[],
            use_advisory_lock=False,
            registry_load_backoff_seconds=0.01,
            registry_load_backoff_cap_seconds=0.01,
        )
        defaults.update(kwargs)
        return QueueJobRunner(**defaults)

    def test_registry_is_not_loaded_while_the_gate_is_closed(self):
        runner = self._make_runner()
        loads = []
        seen_before_load = []
        gate = _FakeGate(
            [False, False, True], on_check=lambda _: seen_before_load.append(len(loads))
        )
        with patch.dict("sys.modules", _registry_module(loads)):
            loaded = runner._load_registry("db1", threading.Event(), gate)
        self.assertTrue(loaded)
        self.assertEqual(loads, ["db1"])
        self.assertEqual(gate.calls, 3)
        # Every check happened before the one load.
        self.assertEqual(seen_before_load, [0, 0, 0])

    def test_stop_while_paused_returns_false_without_loading(self):
        runner = self._make_runner()
        stop_event = threading.Event()
        loads = []

        def stop_on_second_check(calls):
            if calls == 2:
                stop_event.set()

        gate = _FakeGate([False], on_check=stop_on_second_check)
        with patch.dict("sys.modules", _registry_module(loads)):
            loaded = runner._load_registry("db1", stop_event, gate)
        self.assertFalse(loaded)
        self.assertEqual(loads, [])

    def test_a_database_error_from_the_gate_backs_off_like_a_registry_error(self):
        runner = self._make_runner()
        loads = []
        gate = _FakeGate([_runner.psycopg2.Error("db unreachable"), True])
        with (
            patch.dict("sys.modules", _registry_module(loads)),
            self.assertLogs(_runner.__name__, "ERROR"),
        ):
            loaded = runner._load_registry("db1", threading.Event(), gate)
        self.assertTrue(loaded)
        self.assertEqual(loads, ["db1"])
        self.assertEqual(runner._failure_timestamps, {})

    def test_without_a_gate_the_registry_loads_immediately(self):
        runner = self._make_runner()
        loads = []
        with patch.dict("sys.modules", _registry_module(loads)):
            self.assertTrue(runner._load_registry("db1", threading.Event()))
        self.assertEqual(loads, ["db1"])


class TestOneGatePerDatabase(unittest.TestCase):
    def test_the_worker_gets_the_gate_the_registry_load_waited_on(self):
        runner = QueueJobRunner(
            database_names=["db1"],
            use_advisory_lock=False,
            worker_keyword_arguments={"concurrency": 2},
            upgrade_gate_keyword_arguments={"interval_seconds": 7},
        )
        gate = _FakeGate([True])
        worker_class = MagicMock()
        loads = []
        seen_while_running = {}

        def run():
            seen_while_running.update(runner._upgrade_gates)

        worker_class.return_value.run.side_effect = run
        with (
            patch.dict("sys.modules", _registry_module(loads)),
            patch.object(_runner, "UpgradeGate", return_value=gate) as gate_class,
            patch.object(_runner, "QueueWorker", worker_class),
        ):
            runner._worker_thread_target("db1", threading.Event())

        gate_class.assert_called_once_with("db1", interval_seconds=7)
        self.assertEqual(gate.calls, 1)
        self.assertIs(worker_class.call_args.kwargs["upgrade_gate"], gate)
        # Visible to the supervisor while the worker runs, gone afterwards.
        self.assertEqual(seen_while_running, {"db1": gate})
        self.assertEqual(runner._upgrade_gates, {})

    def test_a_finishing_thread_does_not_drop_its_successors_gate(self):
        runner = QueueJobRunner(database_names=["db1"], use_advisory_lock=False)
        successor = _FakeGate([True])

        def load(db_name, stop_event, upgrade_gate):
            # A replacement thread registered its gate while this one ran.
            runner._upgrade_gates[db_name] = successor
            return False

        with (
            patch.object(_runner, "UpgradeGate", return_value=_FakeGate([True])),
            patch.object(runner, "_load_registry", side_effect=load),
        ):
            runner._worker_thread_target("db1", threading.Event())
        self.assertIs(runner._upgrade_gates["db1"], successor)


class TestPauseHealth(unittest.TestCase):
    def test_an_ordinary_pause_is_healthy(self):
        runner = QueueJobRunner(database_names=[], use_advisory_lock=False)
        runner._upgrade_gates["db1"] = _FakeGate([False], overdue=False)
        self.assertFalse(runner._fleet_is_degraded())

    def test_an_overdue_pause_is_degraded(self):
        runner = QueueJobRunner(database_names=[], use_advisory_lock=False)
        runner._upgrade_gates["db1"] = _FakeGate([False], overdue=True)
        self.assertTrue(runner._fleet_is_degraded())

    def test_an_overdue_pause_is_degraded_even_with_db_error_signal_off(self):
        runner = QueueJobRunner(
            database_names=[],
            use_advisory_lock=False,
            database_unhealthy_after_seconds=0,
        )
        runner._upgrade_gates["db1"] = _FakeGate([False], overdue=True)
        self.assertTrue(runner._fleet_is_degraded())

    def test_an_overdue_pause_stops_writing_the_heartbeat(self):
        runner = QueueJobRunner(database_names=[], use_advisory_lock=False)
        runner._upgrade_gates["db1"] = _FakeGate([False], overdue=True)
        with patch.object(_runner, "write_heartbeat") as write:
            runner._update_heartbeat()
        write.assert_not_called()


class TestRunInstallsTheProcessGuard(unittest.TestCase):
    def test_run_installs_the_guard_before_anything_else(self):
        runner = QueueJobRunner(database_names=["db1"], use_advisory_lock=False)
        runner.stop_event.set()
        with patch.object(_runner.process_guards, "install") as install:
            runner.run()
        install.assert_called_once_with()


class TestGateSettingsFromTheEnvironment(unittest.TestCase):
    def _runner_from_env(self, **env):
        clean = {name: value for name, value in os.environ.items()}
        for name in GATE_ENV_VARS:
            clean.pop(name, None)
        clean.update(env)
        with (
            patch.object(_runner, "odoo") as mock_odoo,
            patch.dict(os.environ, clean, clear=True),
        ):
            mock_odoo.tools.config = {"db_name": ""}
            return QueueJobRunner.from_environ_or_config()

    def test_defaults(self):
        runner = self._runner_from_env()
        self.assertEqual(
            runner.upgrade_gate_keyword_arguments,
            {
                "enabled": True,
                "version_check": True,
                "interval_seconds": 5.0,
                "unhealthy_after_seconds": 3600.0,
            },
        )

    def test_overrides(self):
        runner = self._runner_from_env(
            JOB_WORKER_UPGRADE_GATE="off",
            JOB_WORKER_UPGRADE_GATE_VERSION_CHECK="0",
            JOB_WORKER_UPGRADE_GATE_INTERVAL="2.5",
            JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER="0",
        )
        self.assertEqual(
            runner.upgrade_gate_keyword_arguments,
            {
                "enabled": False,
                "version_check": False,
                "interval_seconds": 2.5,
                "unhealthy_after_seconds": 0.0,
            },
        )

    def test_invalid_flag_names_the_variable(self):
        with self.assertRaisesRegex(ValueError, "JOB_WORKER_UPGRADE_GATE\\b"):
            self._runner_from_env(JOB_WORKER_UPGRADE_GATE="maybe")

    def test_invalid_number_names_the_variable(self):
        with self.assertRaisesRegex(ValueError, "JOB_WORKER_UPGRADE_GATE_INTERVAL"):
            self._runner_from_env(JOB_WORKER_UPGRADE_GATE_INTERVAL="soon")

    def test_out_of_range_number_is_rejected(self):
        with self.assertRaisesRegex(
            ValueError, "JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER"
        ):
            self._runner_from_env(JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER="-1")
        with self.assertRaisesRegex(ValueError, "JOB_WORKER_UPGRADE_GATE_INTERVAL"):
            self._runner_from_env(JOB_WORKER_UPGRADE_GATE_INTERVAL="0")


if __name__ == "__main__":
    unittest.main()
