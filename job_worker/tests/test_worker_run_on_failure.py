"""The worker must honour ``run_on_failure``, exactly as the ORM path does.

``run_on_failure`` marks a job as an ``on_error()`` handler: it runs only when
its parent / dependencies fail, and is cancelled when they succeed.
``models/queue_job.py`` implements that. ``cli/worker.py`` did not, so the two
disagreed in both directions:

* on success the worker promoted the ``on_error`` handler to ``pending``, so it
  *ran* after a successful job;
* on failure the worker marked it ``failed``, so the handler that exists
  precisely to clean up after a failure never ran.

Every test takes the ORM path (``run_now()``, the Run-now button,
``queue_job__no_delay``), so a green suite proved nothing about either. These
tests drive the worker.
"""

from contextlib import contextmanager

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerRunOnFailure(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def _ok_job(self, env, name, **kw):
        partner = env["res.partner"].create({"name": name})
        return env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": name + " done"}],
            kwargs={},
            channel="rof",
            **kw,
        )

    def _boom_job(self, env, name, **kw):
        """A job whose method does not exist -> permanent failure."""
        partner = env["res.partner"].create({"name": name})
        job = env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="no_such_method_for_tests",
            record_ids=partner.ids,
            args=[],
            kwargs={},
            channel="rof",
            max_retries=1,
            **kw,
        )
        # handle_exception increments attempts before comparing, and
        # max_retries=0 means retry forever, so exhaust the budget up front.
        job.attempts = 1
        return job

    # --- group barriers -------------------------------------------------

    def test_group_success_cancels_the_on_error_twin(self):
        with self._external_env() as (cr, env):
            member = self._ok_job(env, "rof member", graph_uuid="g-ok")
            on_done = self._ok_job(
                env,
                "rof on_done",
                graph_uuid="g-ok",
                dependency_job_ids=[member.id],
            )
            on_error = self._ok_job(
                env,
                "rof on_error",
                graph_uuid="g-ok",
                dependency_job_ids=[member.id],
                run_on_failure=True,
            )

            QueueWorker(cr.dbname).execute_job(cr, member.id)

            on_done.invalidate_recordset()
            on_error.invalidate_recordset()
            self.assertEqual(on_done.state, "pending")
            self.assertEqual(
                on_error.state,
                "cancelled",
                "on_error handler must not run after a successful group",
            )
            self.assertTrue(on_error.cancelled_at)

    def test_group_failure_promotes_the_on_error_twin(self):
        with self._external_env() as (cr, env):
            member = self._boom_job(env, "rof boom", graph_uuid="g-bad")
            on_done = self._ok_job(
                env,
                "rof on_done 2",
                graph_uuid="g-bad",
                dependency_job_ids=[member.id],
            )
            on_error = self._ok_job(
                env,
                "rof on_error 2",
                graph_uuid="g-bad",
                dependency_job_ids=[member.id],
                run_on_failure=True,
            )

            QueueWorker(cr.dbname).execute_job(cr, member.id)

            member.invalidate_recordset()
            on_done.invalidate_recordset()
            on_error.invalidate_recordset()
            self.assertEqual(member.state, "failed")
            self.assertEqual(on_done.state, "failed")
            self.assertEqual(
                on_error.state,
                "pending",
                "on_error handler must run when the group fails",
            )

    # --- parent_id chains -----------------------------------------------

    def test_chain_success_cancels_the_on_error_child(self):
        with self._external_env() as (cr, env):
            parent = self._ok_job(env, "rof parent")
            child_ok = self._ok_job(env, "rof child ok", parent_id=parent.id)
            child_err = self._ok_job(
                env, "rof child err", parent_id=parent.id, run_on_failure=True
            )

            QueueWorker(cr.dbname).execute_job(cr, parent.id)

            child_ok.invalidate_recordset()
            child_err.invalidate_recordset()
            self.assertEqual(child_ok.state, "pending")
            self.assertEqual(
                child_err.state,
                "cancelled",
                "on_error child must not run after a successful parent",
            )

    # --- raw-SQL control-plane paths -------------------------------------
    # These run without an ORM env (heartbeat thread / acquire loop), so they
    # carry their own copy of the cascade and need covering separately.

    def test_timeout_promotes_the_on_error_dependents(self):
        with self._external_env() as (cr, env):
            parent = self._ok_job(env, "rof timeout parent", graph_uuid="g-to")
            child_err = self._ok_job(
                env, "rof timeout child err", parent_id=parent.id, run_on_failure=True
            )
            child_ok = self._ok_job(env, "rof timeout child ok", parent_id=parent.id)
            barrier_err = self._ok_job(
                env,
                "rof timeout barrier err",
                graph_uuid="g-to",
                dependency_job_ids=[parent.id],
                run_on_failure=True,
            )
            # _handle_timeout only acts on a 'started' job, and permanently
            # fails it once the retry budget is spent.
            parent.write({"state": "started", "max_retries": 1, "attempts": 1})
            env.cr.commit()

            QueueWorker(cr.dbname)._handle_timeout(parent.id, 1)

            for rec in (parent, child_err, child_ok, barrier_err):
                rec.invalidate_recordset()
            self.assertEqual(parent.state, "failed")
            self.assertEqual(child_ok.state, "failed")
            self.assertEqual(
                child_err.state,
                "pending",
                "on_error child must run when the parent times out",
            )
            self.assertEqual(
                barrier_err.state,
                "pending",
                "on_error barrier must run when the parent times out",
            )

    def test_chain_failure_promotes_the_on_error_child(self):
        with self._external_env() as (cr, env):
            parent = self._boom_job(env, "rof parent boom")
            child_ok = self._ok_job(env, "rof child ok 2", parent_id=parent.id)
            child_err = self._ok_job(
                env, "rof child err 2", parent_id=parent.id, run_on_failure=True
            )

            QueueWorker(cr.dbname).execute_job(cr, parent.id)

            parent.invalidate_recordset()
            child_ok.invalidate_recordset()
            child_err.invalidate_recordset()
            self.assertEqual(parent.state, "failed")
            self.assertEqual(child_ok.state, "failed")
            self.assertEqual(
                child_err.state,
                "pending",
                "on_error child must run when the parent fails",
            )
