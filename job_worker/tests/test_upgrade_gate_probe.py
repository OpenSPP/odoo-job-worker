"""Integration tests for the upgrade gate and the process guard on a real database.

The pure-Python tests in ``test_upgrade_gate.py`` cover the decision logic
against a mocked cursor. These run the gate's real probe SQL, read real
manifests from the addons path, and install the real process guard into
Odoo's ``odoo.modules.loading``.

Probe tests run on the test transaction (``self.env.cr``) so the module-state
changes they make are rolled back with it and never reach another process.
"""

import odoo
from odoo.tests.common import TransactionCase, tagged

from odoo.addons.job_worker.cli import process_guards, upgrade_gate

GATE_LOGGER = "odoo.addons.job_worker.cli.upgrade_gate"


@tagged("post_install", "-at_install")
class TestUpgradeGateProbe(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.code_versions = upgrade_gate.addon_code_versions()

    def _probe(self, code_versions="default"):
        if code_versions == "default":
            code_versions = self.code_versions
        return upgrade_gate.probe(self.env.cr, code_versions)

    def test_installed_database_is_open(self):
        state = self._probe()
        self.assertTrue(state.open, state)
        self.assertEqual(state.missing, ())

    def test_job_worker_version_comes_from_its_manifest(self):
        self.env.cr.execute(
            "SELECT latest_version FROM ir_module_module WHERE name = 'job_worker'"
        )
        self.assertEqual(self.code_versions["job_worker"], self.env.cr.fetchone()[0])

    def test_module_to_upgrade_closes_the_gate(self):
        self.env.cr.execute(
            "UPDATE ir_module_module SET state = 'to upgrade' WHERE name = 'job_worker'"
        )
        state = self._probe()
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_MODULES_IN_TRANSITION)
        self.assertIn("job_worker:to upgrade", state.details)

    def test_module_to_install_closes_the_gate(self):
        self.env.cr.execute(
            "UPDATE ir_module_module SET state = 'to install'"
            " WHERE name = (SELECT name FROM ir_module_module"
            "               WHERE state = 'uninstalled' ORDER BY name LIMIT 1)"
            " RETURNING name"
        )
        name = self.env.cr.fetchone()[0]
        state = self._probe()
        self.assertEqual(state.reason, upgrade_gate.REASON_MODULES_IN_TRANSITION)
        self.assertIn(f"{name}:to install", state.details)

    def test_partial_update_flag_closes_the_gate(self):
        self.env.cr.execute(
            "INSERT INTO ir_config_parameter (key, value)"
            " VALUES ('base.partially_updated_database', '1')"
        )
        state = self._probe()
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_PARTIAL_UPDATE)

    def test_version_skew_closes_the_gate(self):
        self.env.cr.execute(
            "UPDATE ir_module_module SET latest_version = '19.0.0.0.1'"
            " WHERE name = 'job_worker'"
        )
        state = self._probe()
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_VERSION_SKEW)
        self.assertEqual(
            state.details,
            (
                f"job_worker: code {self.code_versions['job_worker']}, "
                "database 19.0.0.0.1",
            ),
        )

    def test_version_check_disabled_ignores_skew(self):
        self.env.cr.execute(
            "UPDATE ir_module_module SET latest_version = '19.0.0.0.1'"
            " WHERE name = 'job_worker'"
        )
        self.assertTrue(self._probe(code_versions=None).open)

    def test_installed_module_missing_from_the_addons_path_is_reported(self):
        versions = dict(self.code_versions)
        del versions["web"]
        state = self._probe(code_versions=versions)
        self.assertTrue(state.open)
        self.assertEqual(state.missing, ("web",))

    def test_gate_uses_its_own_connection(self):
        # The default cursor factory must work without a registry and see only
        # committed state: the uncommitted skew below is invisible to it.
        self.env.cr.execute(
            "UPDATE ir_module_module SET latest_version = '19.0.0.0.1'"
            " WHERE name = 'job_worker'"
        )
        gate = upgrade_gate.UpgradeGate(self.env.cr.dbname)
        self.assertTrue(gate.is_open())

    def test_probe_blocked_by_a_schema_lock_fails_closed(self):
        # What an upgrade altering ir_module_module looks like to the gate.
        self.env.cr.execute("LOCK TABLE ir_module_module IN ACCESS EXCLUSIVE MODE")
        gate = upgrade_gate.UpgradeGate(
            self.env.cr.dbname, lock_timeout_ms=200, statement_timeout_ms=1000
        )
        with self.assertLogs(GATE_LOGGER, "WARNING") as logs:
            state = gate.check()
        self.assertFalse(state.open)
        self.assertEqual(state.reason, upgrade_gate.REASON_PROBE_BLOCKED)
        self.assertIn(self.env.cr.dbname, logs.records[0].getMessage())


@tagged("post_install", "-at_install")
class TestProcessGuardInOdoo(TransactionCase):
    def test_registry_load_failure_path_imports_the_guard(self):
        original = process_guards.install()
        self.assertIsNotNone(original)
        self.addCleanup(setattr, odoo.modules.loading, "reset_modules_state", original)

        # ``Registry.new()`` resolves the function exactly like this, at call
        # time, inside its ``except`` block.
        from odoo.modules.loading import reset_modules_state

        self.assertIsNot(reset_modules_state, original)
        with self.assertLogs(process_guards.__name__, "WARNING") as logs:
            reset_modules_state(self.env.cr.dbname)
        self.assertIn("NOT resetting", logs.records[0].getMessage())
