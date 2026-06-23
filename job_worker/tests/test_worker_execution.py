from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker
from ..exception import RetryableJobError


@tagged("post_install", "-at_install")
class TestWorkerExecution(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def test_execute_job_success_on_record_method(self):
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            partner = env["res.partner"].create({"name": "Before Execute"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "After Execute"}],
                kwargs={},
                channel="exec_record",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed_job = env["queue.job"].browse(job.id)
            refreshed_partner = env["res.partner"].browse(partner.id)
            self.assertEqual(refreshed_job.state, "done")
            self.assertEqual(refreshed_partner.name, "After Execute")

    def test_execute_job_success_on_model_method(self):
        with self._external_env() as (cr, env):
            partner_name = "Created From Queue"
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="create",
                record_ids=[],
                args=[{"name": partner_name}],
                kwargs={},
                channel="exec_model",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed_job = env["queue.job"].browse(job.id)
            created = env["res.partner"].search([("name", "=", partner_name)], limit=1)
            self.assertEqual(refreshed_job.state, "done")
            self.assertTrue(created)

    def test_execute_job_retries_with_exponential_backoff_then_fails(self):
        with self._external_env() as (cr, env):
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=2,
                channel="retry",
            )
            worker = QueueWorker(cr.dbname)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            after_first = env["queue.job"].browse(job.id)
            delay_first = (
                fields.Datetime.to_datetime(after_first.scheduled_at)
                - fields.Datetime.now()
            )
            self.assertEqual(after_first.state, "pending")
            self.assertEqual(after_first.attempts, 1)
            self.assertTrue(delay_first.total_seconds() >= 5)
            self.assertTrue(after_first.exc_info)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            after_second = env["queue.job"].browse(job.id)
            delay_second = (
                fields.Datetime.to_datetime(after_second.scheduled_at)
                - fields.Datetime.now()
            )
            self.assertEqual(after_second.state, "pending")
            self.assertEqual(after_second.attempts, 2)
            self.assertTrue(delay_second.total_seconds() >= 15)
            self.assertTrue(after_second.exc_info)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            after_third = env["queue.job"].browse(job.id)
            self.assertEqual(after_third.state, "failed")
            self.assertEqual(after_third.attempts, 3)
            self.assertTrue(after_third.exc_info)

    def test_execute_job_retries_forever_when_max_retries_is_zero(self):
        with self._external_env() as (cr, env):
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=0,
                channel="retry_zero",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)
            worker.execute_job(cr, job.id)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed_job = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed_job.state, "pending")
            self.assertEqual(refreshed_job.attempts, 3)
            self.assertTrue(refreshed_job.exc_info)

    def test_handle_exception_uses_exact_exponential_backoff_steps(self):
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=5,
                channel="backoff_exact",
            )

            fixed_1 = datetime(2026, 1, 1, 0, 0, 0)
            with patch("odoo.fields.Datetime.now", return_value=fixed_1):
                worker.handle_exception(job)
            env.invalidate_all()
            first = env["queue.job"].browse(job.id)
            self.assertEqual(
                fields.Datetime.to_datetime(first.scheduled_at),
                fixed_1 + timedelta(seconds=10),
            )

            fixed_2 = datetime(2026, 1, 1, 0, 1, 0)
            with patch("odoo.fields.Datetime.now", return_value=fixed_2):
                worker.handle_exception(first)
            env.invalidate_all()
            second = env["queue.job"].browse(job.id)
            self.assertEqual(
                fields.Datetime.to_datetime(second.scheduled_at),
                fixed_2 + timedelta(seconds=20),
            )

    def test_handle_exception_caps_backoff_for_large_attempts(self):
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname, max_backoff_seconds=60)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=0,
                channel="backoff_cap",
            )
            job.write({"attempts": 10})

            fixed = datetime(2026, 1, 1, 0, 0, 0)
            with patch("odoo.fields.Datetime.now", return_value=fixed):
                worker.handle_exception(job)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(
                fields.Datetime.to_datetime(refreshed.scheduled_at),
                fixed + timedelta(seconds=60),
            )

    def test_handle_exception_clears_scheduled_at_on_terminal_failure(self):
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=1,
                channel="backoff_terminal",
            )
            job.write(
                {
                    "attempts": 1,
                    "scheduled_at": fields.Datetime.now() + timedelta(hours=1),
                }
            )

            worker.handle_exception(job)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "failed")
            self.assertFalse(refreshed.scheduled_at)

    def test_run_now_executes_job_in_current_transaction(self):
        partner_name = "Run Now Partner"
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": partner_name}],
            kwargs={},
            channel="run_now",
        )

        job.run_now()

        self.assertEqual(job.state, "done")
        partner = self.env["res.partner"].search([("name", "=", partner_name)], limit=1)
        self.assertTrue(partner)

    def test_run_now_marks_failed_and_reraises(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="run_now_fail",
        )

        try:
            job.run_now()
            self.fail("run_now() should raise when method does not exist")
        except AttributeError:
            pass

        job.invalidate_recordset()
        self.assertEqual(job.state, "failed")
        self.assertIn("AttributeError", job.exc_info or "")

    def test_execute_job_injects_job_uuid_in_method_context(self):
        with self._external_env() as (cr, env):
            partner = env["res.partner"].create({"name": "Missing Job UUID"})

            def _capture_job_uuid(records):
                records.ensure_one()
                records.write({"name": str(records.env.context.get("job_uuid"))})
                return True

            with patch.object(
                type(env["res.partner"]),
                "queue_capture_job_uuid",
                _capture_job_uuid,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_capture_job_uuid",
                    record_ids=partner.ids,
                    args=[],
                    kwargs={},
                    channel="ctx_job_uuid",
                )
                worker = QueueWorker(cr.dbname)
                worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["res.partner"].browse(partner.id)
            self.assertEqual(refreshed.name, str(job.uuid))

    def test_execute_job_missing_target_record_retries_then_fails(self):
        with self._external_env() as (cr, env):
            partner = env["res.partner"].create({"name": "Missing target"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Should Fail"}],
                kwargs={},
                max_retries=1,
                channel="missing_target",
            )
            partner.unlink()
            worker = QueueWorker(cr.dbname)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            first = env["queue.job"].browse(job.id)
            self.assertEqual(first.state, "pending")
            self.assertEqual(first.attempts, 1)
            self.assertIn("not found", first.exc_info or "")

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            second = env["queue.job"].browse(job.id)
            self.assertEqual(second.state, "failed")
            self.assertEqual(second.attempts, 2)
            self.assertIn("not found", second.exc_info or "")

    def test_execute_job_missing_model_is_retried_not_permanently_failed(self):
        """A model absent from the worker's registry is a transient lag, not
        a failure: re-queue without consuming the attempt budget.

        The worker is a separate process that builds its registry by importing
        module code from its own addons-path. Right after a module is installed
        or updated from the UI (before the worker's signaling reload lands), or
        while the worker is mid-redeploy, a model can be present on the app
        server yet absent here. A bare lookup would raise KeyError, be treated
        as a normal failure, and burn the whole retry budget — permanently
        failing a job that would succeed seconds later once the registry
        reloads. Instead the worker re-queues with ``ignore_retry`` and
        expedites the next signaling check.
        """
        with self._external_env() as (cr, env):
            partner = env["res.partner"].create({"name": "Lagging Worker"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "x"}],
                kwargs={},
                max_retries=3,
                channel="missing_model",
            )
            # Simulate a worker whose registry lags the app server: rewrite the
            # stored payload to a model that is not in the registry. Only nested
            # odoo_recordset objects are dereferenced on decode, so an empty
            # args/ids payload with a bogus model string decodes cleanly.
            # ``payload`` is a fields.Json column → read/write plain dicts.
            payload = dict(job.payload)
            payload["model"] = "this.model.is.not.loaded.yet"
            payload["args"] = []
            payload["ids"] = []
            job.payload = payload
            job.env.cr.flush()

            worker = QueueWorker(cr.dbname)
            worker._last_registry_check = 999999.0  # sentinel: must be reset to 0
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            # Re-queued, not failed; attempt budget untouched (ignore_retry).
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 0)
            self.assertTrue(refreshed.scheduled_at)
            self.assertIn("registry", (refreshed.exc_info or "").lower())
            # The next registry signaling check was expedited.
            self.assertEqual(worker._last_registry_check, 0.0)

    def test_execute_job_missing_model_fails_normally_past_grace_window(self):
        """Past the registry-lag grace window, a still-missing model stops being
        treated as transient lag and fails through the normal retry budget —
        so a genuinely-missing model (typo / uninstalled) does not re-queue
        forever. Driven here by backdating the job past the grace window.
        """
        with self._external_env() as (cr, env):
            partner = env["res.partner"].create({"name": "Never Deployed"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "x"}],
                kwargs={},
                max_retries=3,
                channel="missing_model_grace",
            )
            payload = dict(job.payload)
            payload["model"] = "this.model.is.not.loaded.yet"
            payload["args"] = []
            payload["ids"] = []
            job.payload = payload
            # Backdate the job well past the default grace window so it is no
            # longer treated as a transiently-lagging worker.
            old = fields.Datetime.now() - timedelta(hours=2)
            env.cr.execute(
                "UPDATE queue_job SET create_date = %s WHERE id = %s",
                (old, job.id),
            )
            env.invalidate_all()

            worker = QueueWorker(cr.dbname)  # default grace = 3600s
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            # Normal failure path: the attempt IS counted (not ignore_retry).
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 1)
            self.assertTrue(refreshed.exc_info)

    def test_button_requeue_resets_job_and_notifies(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="requeue",
        )
        job.write(
            {
                "state": "failed",
                "attempts": 4,
                "exc_info": "boom",
                "scheduled_at": fields.Datetime.now() + timedelta(hours=1),
            }
        )

        with patch.object(self.env.cr, "execute", wraps=self.env.cr.execute) as execute:
            job.button_requeue()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)
        self.assertFalse(job.exc_info)
        self.assertFalse(job.scheduled_at)
        notify_calls = [
            call
            for call in execute.call_args_list
            if call.args
            and isinstance(call.args[0], str)
            and "NOTIFY queue_job_wake_up" in call.args[0]
        ]
        self.assertTrue(notify_calls, "Expected a NOTIFY queue_job_wake_up query")

    def test_button_requeue_clears_worker_ownership_metadata(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="requeue_owner_reset",
        )
        job.write(
            {
                "state": "failed",
                "worker_id": "worker-old",
                "heartbeat": fields.Datetime.now(),
            }
        )

        job.button_requeue()
        job.invalidate_recordset()

        self.assertFalse(job.worker_id)
        self.assertFalse(job.heartbeat)

    def test_run_now_releases_waiting_children(self):
        """Completing a parent via run_now() releases waiting children."""
        partner = self.env["res.partner"].create({"name": "Release Test"})
        parent_job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Release After"}],
            kwargs={},
        )
        child_job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Child After"}],
            kwargs={},
            parent_id=parent_job.id,
        )
        self.assertEqual(child_job.state, "waiting")

        parent_job.run_now()

        child_job.invalidate_recordset()
        self.assertEqual(parent_job.state, "done")
        self.assertEqual(child_job.state, "pending")

    def test_run_now_cascades_failure_to_waiting_children(self):
        """Failing a parent via run_now() cascades failure to children."""
        parent_job = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
        )
        child_job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=[],
            args=[{"name": "Child Should Fail"}],
            kwargs={},
            parent_id=parent_job.id,
        )
        self.assertEqual(child_job.state, "waiting")

        try:
            parent_job.run_now()
        except AttributeError:
            pass

        parent_job.invalidate_recordset()
        child_job.invalidate_recordset()
        self.assertEqual(parent_job.state, "failed")
        self.assertEqual(child_job.state, "failed")
        self.assertIn("Parent job", child_job.exc_info or "")

    def test_worker_result_captured(self):
        """Worker execute_job() populates the result field."""
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            partner = env["res.partner"].create({"name": "Worker Result"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Worker Result After"}],
                kwargs={},
                channel="worker_result",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed_job = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed_job.state, "done")
            # res.partner.write() returns True
            self.assertTrue(refreshed_job.result)

    def test_worker_releases_waiting_children(self):
        """Worker execute_job() releases waiting children on success."""
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            partner = env["res.partner"].create({"name": "Worker Release"})
            parent_job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Worker Release After"}],
                kwargs={},
                channel="worker_release",
            )
            child_job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Worker Child After"}],
                kwargs={},
                channel="worker_release",
                parent_id=parent_job.id,
            )
            self.assertEqual(child_job.state, "waiting")

            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, parent_job.id)

            env.invalidate_all()
            refreshed_child = env["queue.job"].browse(child_job.id)
            self.assertEqual(refreshed_child.state, "pending")

    def test_worker_cascades_failure_to_waiting_children(self):
        """Worker cascades permanent failure to waiting children."""
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            parent_job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=1,
                channel="worker_cascade",
            )
            child_job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "Worker Cascade Child"}],
                kwargs={},
                channel="worker_cascade",
                parent_id=parent_job.id,
            )

            worker = QueueWorker(cr.dbname)
            # First attempt — retries
            worker.execute_job(cr, parent_job.id)
            env.invalidate_all()
            self.assertEqual(env["queue.job"].browse(child_job.id).state, "waiting")

            # Second attempt — permanent failure
            worker.execute_job(cr, parent_job.id)
            env.invalidate_all()
            refreshed_parent = env["queue.job"].browse(parent_job.id)
            refreshed_child = env["queue.job"].browse(child_job.id)
            self.assertEqual(refreshed_parent.state, "failed")
            self.assertEqual(refreshed_child.state, "failed")
            self.assertIn("Parent job", refreshed_child.exc_info or "")

    def test_update_heartbeats_only_updates_owned_jobs(self):
        with self._external_env() as (cr, env):
            old_heartbeat = fields.Datetime.now() - timedelta(minutes=3)
            worker = QueueWorker(cr.dbname)
            owned = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel="heartbeat",
            )
            foreign = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel="heartbeat",
            )
            owned.write(
                {
                    "state": "started",
                    "worker_id": worker.worker_uuid,
                    "heartbeat": old_heartbeat,
                }
            )
            foreign.write(
                {
                    "state": "started",
                    "worker_id": "foreign-worker",
                    "heartbeat": old_heartbeat,
                }
            )
            cr.commit()
            worker.active_job_ids = {owned.id, foreign.id}
            worker.update_heartbeats()

            cr.commit()
            env.invalidate_all()
            self.assertTrue(env["queue.job"].browse(owned.id).heartbeat > old_heartbeat)
            self.assertEqual(
                env["queue.job"].browse(foreign.id).heartbeat, old_heartbeat
            )


@tagged("post_install", "-at_install")
class TestWorkerExecutionEdgeCases(TransactionCase):
    """Probe worker execution paths for adversarial scenarios."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def test_worker_execute_job_with_deleted_target_record(self):
        """Worker should handle executing a job whose target record
        was deleted between enqueue and execution."""
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            partner = env["res.partner"].create({"name": "Will Be Deleted"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "After Delete"}],
                kwargs={},
                max_retries=1,
                channel="deleted",
            )
            partner.unlink()
            cr.commit()

            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            # Should retry then fail
            self.assertIn(refreshed.state, ("pending", "failed"))
            self.assertIn("not found", refreshed.exc_info or "")

    def test_worker_execute_job_with_zero_timeout(self):
        """Worker should treat timeout=0 as no timeout."""
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            partner = env["res.partner"].create({"name": "Zero Timeout"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Zero Timeout After"}],
                kwargs={},
                timeout=0,
                channel="zero_timeout",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "done")

    def test_worker_handle_exception_retryable_with_both_flags(self):
        """RetryableJobError with ignore_retry=True and explicit seconds."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "both flags"}],
                kwargs={},
                max_retries=1,
                channel="both_flags",
            )
            exc = RetryableJobError("transient", seconds=30, ignore_retry=True)
            worker.handle_exception(job, exc=exc)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 0)
            diff = abs(
                (
                    fields.Datetime.to_datetime(refreshed.scheduled_at)
                    - fields.Datetime.now()
                ).total_seconds()
                - 30
            )
            self.assertLess(diff, 5)

    def test_worker_handle_exception_non_retryable_clears_ownership(self):
        """Terminal failure should clear worker_id and heartbeat."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "clear ownership"}],
                kwargs={},
                max_retries=1,
                channel="clear_own",
            )
            job.write(
                {
                    "attempts": 1,
                    "worker_id": "test-worker",
                    "heartbeat": fields.Datetime.now(),
                }
            )
            worker.handle_exception(job, exc=Exception("boom"))
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "failed")
            self.assertFalse(refreshed.worker_id)
            self.assertFalse(refreshed.heartbeat)


@tagged("post_install", "-at_install")
class TestRunNowStateGuard(TransactionCase):
    """Probe run_now() behavior when called on jobs in unexpected states."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_run_now_on_done_job_re_executes(self):
        """run_now() has no state guard -- calling it on a done job
        will re-execute the payload and overwrite the state."""
        partner = self.env["res.partner"].create({"name": "Before"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "After"}],
            kwargs={},
            channel="done_rerun",
        )
        # First run
        fixed_first = datetime(2026, 1, 1, 12, 0, 0)
        with patch("odoo.fields.Datetime.now", return_value=fixed_first):
            job.run_now()
        self.assertEqual(job.state, "done")

        # Second run with a different timestamp
        fixed_second = datetime(2026, 1, 1, 13, 0, 0)
        with patch("odoo.fields.Datetime.now", return_value=fixed_second):
            job.run_now()
        self.assertEqual(job.state, "done")
        # completed_at was updated to the second run's time
        self.assertEqual(fields.Datetime.to_datetime(job.completed_at), fixed_second)

    def test_run_now_on_cancelled_job_re_executes(self):
        """run_now() on a cancelled job will re-execute (no guard)."""
        partner = self.env["res.partner"].create({"name": "Cancelled"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Revived"}],
            kwargs={},
            channel="cancelled_rerun",
        )
        job.button_cancelled()
        self.assertEqual(job.state, "cancelled")

        # run_now re-executes regardless of state
        job.run_now()
        self.assertEqual(job.state, "done")
