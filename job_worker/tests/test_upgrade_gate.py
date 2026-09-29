"""Tests for the upgrade gate and the job-worker process guard.

Pure-Python / unittest tests that do not require Odoo. They exercise:

* ``job_worker.cli.upgrade_gate.probe`` — turning the probe query's row into
  an open/closed decision.
* ``job_worker.cli.upgrade_gate.UpgradeGate`` — caching, fail-closed handling
  of a blocked probe, transition logging and the overdue signal.
* ``job_worker.cli.process_guards.install`` — replacing Odoo's
  ``reset_modules_state`` in the job-worker process.

They are run via ``run_runner_tests.py``, not the Odoo test runner. The probe
SQL itself is exercised against a real database by
``test_upgrade_gate_probe.py``.
"""

import importlib.util
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_helpers_spec = importlib.util.spec_from_file_location(
    "_runner_test_helpers", _helpers_path
)
_helpers = importlib.util.module_from_spec(_helpers_spec)
_helpers_spec.loader.exec_module(_helpers)
_helpers.load_runner()

upgrade_gate = sys.modules["job_worker.cli.upgrade_gate"]
process_guards = sys.modules["job_worker.cli.process_guards"]
psycopg2_stub = sys.modules["psycopg2"]

GATE_LOGGER = upgrade_gate.__name__


def _pg_error(pgcode):
    """A psycopg2-style error carrying ``pgcode``."""
    error = psycopg2_stub.OperationalError(f"pgcode {pgcode}")
    error.pgcode = pgcode
    return error


def _cursor(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    return cursor


# transient, partial, skew, missing
NOTHING = (None, False, None, None)


class TestProbe(unittest.TestCase):
    def test_open_when_nothing_to_report(self):
        state = upgrade_gate.probe(_cursor(NOTHING), {"base": "19.0.1.3"})
        self.assertTrue(state.open)
        self.assertIsNone(state.reason)
        self.assertEqual(state.details, ())
        self.assertEqual(state.missing, ())

    def test_closed_when_modules_are_in_transition(self):
        row = (["spp_programs:to upgrade"], False, None, None)
        state = upgrade_gate.probe(_cursor(row), {})
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_MODULES_IN_TRANSITION)
        self.assertEqual(state.details, ("spp_programs:to upgrade",))

    def test_closed_when_partial_update_flag_is_set(self):
        state = upgrade_gate.probe(_cursor((None, True, None, None)), {})
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_PARTIAL_UPDATE)

    def test_closed_on_version_skew(self):
        skew = ["job_worker: code 19.0.1.3.0, database 19.0.1.2.0"]
        state = upgrade_gate.probe(_cursor((None, False, skew, None)), {})
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_VERSION_SKEW)
        self.assertEqual(state.details, tuple(skew))

    def test_modules_in_transition_take_precedence(self):
        # An upgrade in progress explains the skew and the flag; report the
        # cause, not its symptoms.
        row = (["a:to upgrade"], True, ["a: code 2, database 1"], None)
        state = upgrade_gate.probe(_cursor(row), {})
        self.assertEqual(state.reason, upgrade_gate.REASON_MODULES_IN_TRANSITION)

    def test_missing_modules_are_reported_but_keep_the_gate_open(self):
        state = upgrade_gate.probe(_cursor((None, False, None, ["spp_gone"])), {})
        self.assertTrue(state.open)
        self.assertEqual(state.missing, ("spp_gone",))

    def test_passes_code_versions_to_the_query(self):
        cursor = _cursor(NOTHING)
        upgrade_gate.probe(cursor, {"b": "19.0.2", "a": "19.0.1"})
        params = cursor.execute.call_args[0][1]
        self.assertTrue(params["check_versions"])
        self.assertEqual(params["names"], ["a", "b"])
        self.assertEqual(params["versions"], ["19.0.1", "19.0.2"])

    def test_version_check_disabled_sends_no_versions(self):
        cursor = _cursor(NOTHING)
        upgrade_gate.probe(cursor, None)
        params = cursor.execute.call_args[0][1]
        self.assertFalse(params["check_versions"])
        self.assertEqual(params["names"], [])
        self.assertEqual(params["versions"], [])


class _Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


class TestUpgradeGate(unittest.TestCase):
    def _gate(self, rows=None, error=None, **kwargs):
        """Build a gate whose probe cursor yields ``rows`` in order."""
        self.cursors = []
        self.connections = []
        rows = list(rows or [NOTHING])

        def connection_factory():
            cursor = MagicMock()
            if error is not None:
                cursor.execute.side_effect = [None, None, error]
            cursor.fetchone.return_value = rows.pop(0) if len(rows) > 1 else rows[0]
            self.cursors.append(cursor)
            connection = MagicMock()
            connection.cursor.return_value = cursor
            self.connections.append(connection)
            return connection

        self.clock = _Clock()
        defaults = dict(
            version_check=True,
            code_versions={"base": "19.0.1.3"},
            interval_seconds=5,
            unhealthy_after_seconds=3600,
            connection_factory=connection_factory,
        )
        defaults.update(kwargs)
        gate = upgrade_gate.UpgradeGate("db1", **defaults)
        gate._clock = self.clock
        return gate

    def test_disabled_gate_is_always_open_and_never_probes(self):
        gate = self._gate(enabled=False)
        self.assertTrue(gate.is_open())
        self.assertEqual(self.cursors, [])

    def test_probe_sets_timeouts_and_never_commits(self):
        gate = self._gate()
        gate.is_open()
        cursor = self.cursors[0]
        statements = [call[0][0] for call in cursor.execute.call_args_list]
        self.assertEqual(statements[0], "SET LOCAL lock_timeout = %s")
        self.assertEqual(statements[1], "SET LOCAL statement_timeout = %s")
        connection = self.connections[0]
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()
        cursor.close.assert_called_once()

    def test_result_is_cached_for_the_interval(self):
        gate = self._gate()
        gate.is_open()
        self.clock.now += 4
        gate.is_open()
        self.assertEqual(len(self.cursors), 1)
        self.clock.now += 2
        gate.is_open()
        self.assertEqual(len(self.cursors), 2)

    def test_force_bypasses_the_cache(self):
        gate = self._gate()
        gate.is_open()
        gate.check(force=True)
        self.assertEqual(len(self.cursors), 2)

    def test_lock_timeout_closes_the_gate(self):
        gate = self._gate(error=_pg_error("55P03"))
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            state = gate.check()
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_PROBE_BLOCKED)
        self.assertEqual(state.details, ("55P03",))

    def test_statement_timeout_closes_the_gate(self):
        gate = self._gate(error=_pg_error("57014"))
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            self.assertFalse(gate.is_open())

    def test_other_database_errors_propagate(self):
        # "Database down" must reach the worker's own recovery and its
        # degraded signal, not hide behind an endless "upgrade" pause.
        gate = self._gate(error=_pg_error(None))
        with self.assertRaises(psycopg2_stub.Error):
            gate.is_open()
        self.connections[0].close.assert_called_once()

    def test_closing_logs_once_with_the_reason_and_database(self):
        closed = (["spp_programs:to upgrade"], False, None, None)
        gate = self._gate(rows=[closed])
        with self.assertLogs(GATE_LOGGER, "WARNING") as logs:
            self.assertFalse(gate.is_open())
        self.assertEqual(len(logs.records), 1)
        message = logs.records[0].getMessage()
        self.assertIn("db1", message)
        self.assertIn("spp_programs:to upgrade", message)
        self.clock.now += 6
        with self.assertNoLogs(GATE_LOGGER, "INFO"):
            self.assertFalse(gate.is_open())

    def test_reopening_logs_resumed(self):
        closed = (["a:to upgrade"], False, None, None)
        gate = self._gate(rows=[closed, NOTHING])
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.clock.now += 252
        with self.assertLogs(GATE_LOGGER, "INFO") as logs:
            self.assertTrue(gate.is_open())
        message = logs.records[-1].getMessage()
        self.assertIn("resumed", message)
        self.assertIn("db1", message)
        self.assertIn("4m12s", message)

    def test_reason_change_while_closed_is_logged(self):
        transient = (["a:to upgrade"], False, None, None)
        skew = (None, False, ["a: code 2, database 1"], None)
        gate = self._gate(rows=[transient, skew])
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.clock.now += 6
        with self.assertLogs(GATE_LOGGER, "WARNING") as logs:
            gate.is_open()
        self.assertIn(upgrade_gate.REASON_VERSION_SKEW, logs.records[0].getMessage())

    def test_still_paused_reminder_every_five_minutes(self):
        closed = (["a:to upgrade"], False, None, None)
        gate = self._gate(rows=[closed])
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.clock.now += 299
        with self.assertNoLogs(GATE_LOGGER, "INFO"):
            gate.is_open()
        # Past both the 300s reminder interval and the 5s probe cache.
        self.clock.now += 6
        with self.assertLogs(GATE_LOGGER, "INFO") as logs:
            gate.is_open()
        self.assertIn("still paused", logs.records[0].getMessage())

    def test_seconds_closed(self):
        closed = (["a:to upgrade"], False, None, None)
        gate = self._gate(rows=[NOTHING, closed])
        gate.is_open()
        self.assertEqual(gate.seconds_closed(), 0.0)
        self.clock.now += 10
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.clock.now += 3
        self.assertEqual(gate.seconds_closed(), 3.0)

    def test_overdue_after_the_threshold_and_logged_once(self):
        closed = (["a:to upgrade"], False, None, None)
        gate = self._gate(rows=[closed], unhealthy_after_seconds=60)
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.assertFalse(gate.is_overdue())
        self.clock.now += 61
        with self.assertLogs(GATE_LOGGER, "ERROR") as logs:
            gate.is_open()
        self.assertTrue(gate.is_overdue())
        self.assertIn("unhealthy", logs.records[0].getMessage())
        self.clock.now += 6
        with self.assertNoLogs(GATE_LOGGER, "ERROR"):
            gate.is_open()

    def test_threshold_zero_is_never_overdue(self):
        closed = (["a:to upgrade"], False, None, None)
        gate = self._gate(rows=[closed], unhealthy_after_seconds=0)
        with self.assertLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()
        self.clock.now += 10**6
        self.assertFalse(gate.is_overdue())

    def test_missing_modules_are_warned_once(self):
        missing = (None, False, None, ["spp_gone"])
        gate = self._gate(rows=[missing])
        with self.assertLogs(GATE_LOGGER, "WARNING") as logs:
            self.assertTrue(gate.is_open())
        self.assertIn("spp_gone", logs.records[0].getMessage())
        self.clock.now += 6
        with self.assertNoLogs(GATE_LOGGER, "WARNING"):
            gate.is_open()

    def test_code_versions_are_read_from_the_addons_path_once(self):
        with patch.object(
            upgrade_gate, "addon_code_versions", return_value={"a": "19.0.1"}
        ) as reader:
            gate = self._gate(code_versions=None)
            gate.is_open()
            self.clock.now += 6
            gate.is_open()
        reader.assert_called_once_with()

    def test_version_check_disabled_never_reads_the_addons_path(self):
        with patch.object(upgrade_gate, "addon_code_versions") as reader:
            gate = self._gate(code_versions=None, version_check=False)
            gate.is_open()
        reader.assert_not_called()


class TestProcessGuards(unittest.TestCase):
    def _fake_odoo_loading(self):
        original = MagicMock(name="reset_modules_state")
        loading = types.ModuleType("odoo.modules.loading")
        loading.reset_modules_state = original
        modules = types.ModuleType("odoo.modules")
        modules.loading = loading
        return modules, loading, original

    def test_install_replaces_reset_modules_state(self):
        modules, loading, original = self._fake_odoo_loading()
        with patch.dict(
            sys.modules, {"odoo.modules": modules, "odoo.modules.loading": loading}
        ):
            returned = process_guards.install()
            self.assertIs(returned, original)
            self.assertIsNot(loading.reset_modules_state, original)
            with self.assertLogs(process_guards.__name__, "WARNING") as logs:
                loading.reset_modules_state("db1")
        original.assert_not_called()
        message = logs.records[0].getMessage()
        self.assertIn("db1", message)
        self.assertIn("NOT resetting", message)

    def test_install_twice_is_a_no_op(self):
        modules, loading, _original = self._fake_odoo_loading()
        with patch.dict(
            sys.modules, {"odoo.modules": modules, "odoo.modules.loading": loading}
        ):
            process_guards.install()
            guard = loading.reset_modules_state
            self.assertIsNone(process_guards.install())
            self.assertIs(loading.reset_modules_state, guard)

    def test_install_without_odoo_does_nothing(self):
        with patch.dict(sys.modules, {"odoo.modules": None}):
            self.assertIsNone(process_guards.install())


if __name__ == "__main__":
    unittest.main()
