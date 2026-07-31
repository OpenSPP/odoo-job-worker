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

import datetime
from contextlib import contextmanager

from odoo import SUPERUSER_ID, api, fields
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

    def test_timeout_reclaim_promotes_the_on_error_dependents(self):
        """A timed-out job that exhausts its retries must still run on_error.

        The cascade moved. ``_handle_timeout`` no longer fails anything — it
        leaves the row ``started`` under its owner so the still-running
        execution thread cannot be doubled up on, and stamps only the reason.
        The permanent-failure decision for a non-completing attempt, and
        therefore this cascade, now belongs to the reclaim branch of
        ``acquire_job_lock`` — the single writer for that outcome. So the test
        drives the reclaim rather than the timeout handler: calling
        ``_handle_timeout`` here would update zero rows (its ``worker_id``
        guard) and assert nothing.
        """
        with self._external_env() as (cr, env):
            # acquire_job_lock takes the first eligible job in the whole table,
            # so clear the field or an unrelated leftover is reclaimed instead.
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
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
            # Stage exactly what a timed-out attempt leaves behind: held
            # 'started' by a worker that never came back, its heartbeat frozen
            # at the moment the timeout fired and now aged past
            # stale_after_seconds, the timeout reason stamped, and the attempt
            # already counted against max_retries by the acquire that started
            # it. The next reclaim is therefore the one that exhausts retries.
            stale_after = 60
            parent.write(
                {
                    "state": "started",
                    "max_retries": 1,
                    "attempts": 1,
                    "worker_id": "worker-that-never-returned",
                    "heartbeat": fields.Datetime.now()
                    - datetime.timedelta(seconds=stale_after * 2),
                    "exc_info": "TimeoutJobError: Job exceeded 1s timeout",
                }
            )
            env.cr.commit()

            reclaimer = QueueWorker(cr.dbname, stale_after_seconds=stale_after)
            self.assertIsNone(
                reclaimer.acquire_job_lock(cr),
                "A timed-out job with no retries left must be failed, not "
                "handed out for another attempt.",
            )

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
