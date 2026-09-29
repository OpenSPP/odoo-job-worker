"""Deploy scenarios — the upgrade gate end to end (Tier 2).

Runs the real ``job_worker_runner.py`` as a subprocess beside module-state
changes and a real one-shot ``odoo -u``, the way a sidecar worker sees an
OpenSPP deploy. Asserts what matters in production:

* no job starts while a module is being upgraded, and the worker resumes on
  its own afterwards;
* a worker started mid-upgrade does not load its registry until the upgrade
  is over;
* code/DB version skew pauses the worker the same way;
* a real ``-u`` beside a busy worker leaves every module installed, every job
  done, and no "Transient module states were reset" from the worker;
* a pause that never ends turns the healthcheck unhealthy, and recovers.

Module states are changed on ``job_worker_stress`` through committed external
cursors, and restored in ``addCleanup`` so the test database is never left
paused.

Depends on ``job_worker_stress``. Tagged ``tier2``; opt-in via
``--test-tags=tier2`` with ``-i job_worker,job_worker_stress``.
"""

import os
import subprocess
import sys
import tempfile
import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from .stress_common import (
    RUNNER_SCRIPT_DEFAULT,
    setup_clean_queue,
    spawn_runner,
    terminate_runner,
    upgrade_module,
    wait_for_terminal_count,
)

MODULE = "job_worker_stress"
GATE_INTERVAL = 0.5
# Long enough for a worker that is mid-select to wake and re-probe the gate.
SETTLE_SECONDS = 3 * GATE_INTERVAL + 1
RUNNER_ENV = {"JOB_WORKER_UPGRADE_GATE_INTERVAL": str(GATE_INTERVAL)}
HEALTHCHECK_SCRIPT = os.path.join(
    os.path.dirname(RUNNER_SCRIPT_DEFAULT), "job_worker_healthcheck.py"
)
RESET_MESSAGE = "Transient module states were reset"


@tagged("post_install", "-at_install", "-standard", "tier2")
class TestDeployUpgradeGate(TransactionCase):
    def setUp(self):
        super().setUp()
        if "job.worker.stress.helper" not in self.env:
            self.skipTest(
                "job_worker_stress addon required for Tier 2 — install via "
                "`-i job_worker,job_worker_stress`"
            )
        setup_clean_queue(self)
        self.db_name = self.env.cr.dbname
        self.channel = f"deploy_{uuid.uuid4().hex[:8]}"
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.limit"].create({"name": self.channel, "limit": 4})
            cr.execute(
                "SELECT state, latest_version FROM ir_module_module WHERE name = %s",
                (MODULE,),
            )
            self._module_snapshot = cr.fetchone()
            cr.commit()
        self.addCleanup(self._restore_module)

    # -- helpers ---------------------------------------------------------

    def _execute(self, query, params=()):
        with self.registry.cursor() as cr:
            cr.execute(query, params)
            cr.commit()

    def _set_module(self, column, value):
        self._execute(
            f"UPDATE ir_module_module SET {column} = %s WHERE name = %s",
            (value, MODULE),
        )

    def _restore_module(self):
        state, latest_version = self._module_snapshot
        self._execute(
            "UPDATE ir_module_module SET state = %s, latest_version = %s"
            " WHERE name = %s",
            (state, latest_version, MODULE),
        )

    def _enqueue(self, count, seconds=0.05):
        with self.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            for _ in range(count):
                env["queue.job"].enqueue(
                    model_name="job.worker.stress.helper",
                    method_name="sleep_for",
                    record_ids=[],
                    args=[seconds],
                    kwargs={},
                    channel=self.channel,
                )
            cr.commit()

    def _count(self, states):
        with self.registry.cursor() as cr:
            cr.execute(
                "SELECT COUNT(*) FROM queue_job WHERE channel = %s AND state IN %s",
                (self.channel, tuple(states)),
            )
            return cr.fetchone()[0]

    def _wait_for_done(self, expected, timeout):
        wait_for_terminal_count(
            self.registry, channel=self.channel, expected=expected, timeout=timeout
        )
        self.assertEqual(self._count(("done",)), expected)

    def _spawn(self, **env):
        return spawn_runner(
            self.db_name, concurrency=2, env_overrides={**RUNNER_ENV, **env}
        )

    @staticmethod
    def _stop(proc):
        stdout, stderr = terminate_runner(proc)
        return (stdout + stderr).decode("utf-8", errors="replace")

    def _healthcheck(self, heartbeat_file, max_age):
        env = dict(os.environ)
        env["JOB_WORKER_HEARTBEAT_FILE"] = heartbeat_file
        env["JOB_WORKER_HEARTBEAT_MAX_AGE"] = str(max_age)
        result = subprocess.run(
            [sys.executable, HEALTHCHECK_SCRIPT], env=env, check=False, timeout=30
        )
        return result.returncode

    def _wait_for_health(self, heartbeat_file, max_age, expected, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._healthcheck(heartbeat_file, max_age) == expected:
                return
            time.sleep(1)
        self.fail(f"healthcheck never returned {expected} within {timeout}s")

    # -- scenarios -------------------------------------------------------

    def test_pauses_while_a_module_is_upgraded_and_resumes(self):
        proc = self._spawn()
        try:
            self._enqueue(2)
            self._wait_for_done(2, timeout=120)

            self._set_module("state", "to upgrade")
            time.sleep(SETTLE_SECONDS)
            self._enqueue(4)
            time.sleep(4)
            self.assertEqual(self._count(("started", "done", "failed")), 2)

            self._set_module("state", "installed")
            self._wait_for_done(6, timeout=60)
        finally:
            output = self._stop(proc)

        self.assertIn(
            f"Job worker paused for database {self.db_name}: modules-in-transition",
            output,
        )
        self.assertIn(f"{MODULE}:to upgrade", output)
        self.assertIn(f"Job worker resumed for database {self.db_name}", output)
        self.assertNotIn(RESET_MESSAGE, output)

    def test_a_worker_started_mid_upgrade_does_not_load_its_registry(self):
        self._set_module("state", "to upgrade")
        proc = self._spawn()
        try:
            self._enqueue(2)
            # Long enough for the runner to boot and reach its first registry
            # load, which the gate must hold back.
            time.sleep(15)
            self.assertEqual(self._count(("started", "done", "failed")), 0)

            self._set_module("state", "installed")
            self._wait_for_done(2, timeout=120)
        finally:
            output = self._stop(proc)

        paused = output.find(f"Job worker paused for database {self.db_name}")
        loading = output.find(f"Loading registry for database {self.db_name}")
        self.assertNotEqual(paused, -1, "the runner never reported the pause")
        self.assertGreater(loading, paused, "the registry loaded before the pause")

    def test_version_skew_pauses_the_worker(self):
        proc = self._spawn()
        try:
            self._enqueue(2)
            self._wait_for_done(2, timeout=120)

            self._set_module("latest_version", "19.0.0.0.1")
            time.sleep(SETTLE_SECONDS)
            self._enqueue(3)
            time.sleep(4)
            self.assertEqual(self._count(("started", "done", "failed")), 2)

            self._restore_module()
            self._wait_for_done(5, timeout=60)
        finally:
            output = self._stop(proc)

        self.assertIn("version-skew", output)
        self.assertIn(f"{MODULE}: code", output)
        self.assertIn("database 19.0.0.0.1", output)

    def _module_write_date(self):
        with self.registry.cursor() as cr:
            cr.execute(
                "SELECT write_date FROM ir_module_module WHERE name = %s", (MODULE,)
            )
            return cr.fetchone()[0]

    def test_a_real_upgrade_beside_a_busy_worker(self):
        proc = self._spawn()
        try:
            self._enqueue(2)
            self._wait_for_done(2, timeout=120)

            self._enqueue(60, seconds=0.1)
            before_upgrade = self._module_write_date()
            upgrade_module(self.db_name, MODULE)
            self._wait_for_done(62, timeout=120)
        finally:
            output = self._stop(proc)

        # The upgrade really ran: it rewrites the module row on completion.
        self.assertGreater(self._module_write_date(), before_upgrade)
        self.assertEqual(self._count(("failed",)), 0)
        with self.registry.cursor() as cr:
            cr.execute(
                "SELECT name, state FROM ir_module_module WHERE state LIKE 'to %%'"
            )
            self.assertEqual(cr.fetchall(), [])
        self.assertNotIn(RESET_MESSAGE, output)
        self.assertNotIn("Traceback", output)

    def test_a_pause_that_never_ends_turns_the_healthcheck_unhealthy(self):
        max_age = 15
        heartbeat_file = os.path.join(
            tempfile.gettempdir(), f"job_worker_heartbeat_{uuid.uuid4().hex}"
        )
        self.addCleanup(
            lambda: os.path.exists(heartbeat_file) and os.remove(heartbeat_file)
        )
        proc = self._spawn(
            JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER=3,
            JOB_WORKER_HEARTBEAT_FILE=heartbeat_file,
            JOB_WORKER_HEARTBEAT_MAX_AGE=max_age,
        )
        try:
            self._wait_for_health(heartbeat_file, max_age, expected=0, timeout=90)

            self._set_module("state", "to upgrade")
            self._wait_for_health(heartbeat_file, max_age, expected=1, timeout=90)

            self._set_module("state", "installed")
            self._wait_for_health(heartbeat_file, max_age, expected=0, timeout=90)
        finally:
            output = self._stop(proc)

        self.assertIn("reporting unhealthy", output)
        self.assertIn(f"Job worker resumed for database {self.db_name}", output)
