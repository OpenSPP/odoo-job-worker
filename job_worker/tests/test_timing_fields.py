from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker
from .common import assert_dt_between, now_with_slack


@tagged("post_install", "-at_install")
class TestTimingFieldsWorker(TransactionCase):
    """Verify started_at / completed_at / duration are set by the worker."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def _clear_active_jobs(self, env):
        env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
            {"state": "done", "heartbeat": False, "worker_id": False}
        )

    def test_worker_sets_started_at_on_acquisition(self):
        """acquire_job_lock sets started_at = NOW()."""
        with self._external_env() as (cr, env):
            self._clear_active_jobs(env)
            partner = env["res.partner"].create({"name": "Timing Start"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Timing After"}],
                kwargs={},
                channel="timing_start",
            )
            cr.commit()
            lower, upper = now_with_slack()

            worker = QueueWorker(cr.dbname)
            acquired_id = worker._acquire_job()
            self.assertEqual(acquired_id, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertTrue(refreshed.started_at)
            assert_dt_between(self, refreshed.started_at, lower, upper)

    def test_worker_sets_completed_at_and_duration_on_success(self):
        """execute_job sets completed_at and duration on done."""
        with self._external_env() as (cr, env):
            self._clear_active_jobs(env)
            partner = env["res.partner"].create({"name": "Timing Success"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Timing Success After"}],
                kwargs={},
                channel="timing_success",
            )

            worker = QueueWorker(cr.dbname)
            acquired_id = worker.acquire_job_lock(cr)
            self.assertEqual(acquired_id, job.id)
            cr.commit()
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "done")
            self.assertTrue(refreshed.started_at)
            self.assertTrue(refreshed.completed_at)
            self.assertGreaterEqual(refreshed.duration, 0.0)
            # Allow minor skew from mixed timestamp sources/precision.
            self.assertGreaterEqual(
                (
                    fields.Datetime.to_datetime(refreshed.completed_at)
                    - fields.Datetime.to_datetime(refreshed.started_at)
                ).total_seconds(),
                -1.0,
            )

    def test_worker_sets_completed_at_and_duration_on_terminal_failure(self):
        """handle_exception sets completed_at/duration on permanent failure."""
        with self._external_env() as (cr, env):
            self._clear_active_jobs(env)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=1,
                channel="timing_fail",
            )
            worker = QueueWorker(cr.dbname)

            # First attempt — retry, no completed_at
            worker.execute_job(cr, job.id)
            env.invalidate_all()
            retry_job = env["queue.job"].browse(job.id)
            self.assertEqual(retry_job.state, "pending")
            self.assertFalse(retry_job.completed_at)
            self.assertFalse(retry_job.duration)

            # Second attempt — permanent failure, completed_at set
            worker.execute_job(cr, job.id)
            env.invalidate_all()
            failed_job = env["queue.job"].browse(job.id)
            self.assertEqual(failed_job.state, "failed")
            self.assertTrue(failed_job.completed_at)
            self.assertGreaterEqual(failed_job.duration, 0.0)

    def test_retry_does_not_set_completed_at(self):
        """On retry (not terminal), completed_at and duration stay empty."""
        with self._external_env() as (cr, env):
            self._clear_active_jobs(env)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=5,
                channel="timing_retry",
            )
            worker = QueueWorker(cr.dbname)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            self.assertFalse(refreshed.completed_at)
            self.assertFalse(refreshed.duration)


@tagged("post_install", "-at_install")
class TestTimingFieldsRunNow(TransactionCase):
    """Verify started_at / completed_at / duration are set by run_now()."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_run_now_sets_timing_on_success(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Timing Run Now"}],
            kwargs={},
            channel="timing_run_now",
        )
        lower, upper = now_with_slack()
        job.run_now()

        self.assertEqual(job.state, "done")
        self.assertTrue(job.started_at)
        self.assertTrue(job.completed_at)
        self.assertGreaterEqual(job.duration, 0.0)
        assert_dt_between(self, job.started_at, lower, upper)
        assert_dt_between(self, job.completed_at, lower, upper)

    def test_run_now_sets_timing_on_failure(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="timing_run_now_fail",
        )
        lower, upper = now_with_slack()
        try:
            job.run_now()
        except AttributeError:
            pass

        self.assertEqual(job.state, "failed")
        self.assertTrue(job.started_at)
        self.assertTrue(job.completed_at)
        self.assertGreaterEqual(job.duration, 0.0)
        assert_dt_between(self, job.started_at, lower, upper)
        assert_dt_between(self, job.completed_at, lower, upper)


@tagged("post_install", "-at_install")
class TestTimingFieldsRequeue(TransactionCase):
    """Verify button_requeue clears timing fields."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_button_requeue_clears_timing_fields(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="timing_requeue",
        )
        job.write(
            {
                "state": "failed",
                "started_at": fields.Datetime.now() - timedelta(minutes=5),
                "completed_at": fields.Datetime.now(),
                "duration": 300.0,
            }
        )

        job.button_requeue()

        self.assertEqual(job.state, "pending")
        self.assertFalse(job.started_at)
        self.assertFalse(job.completed_at)
        self.assertFalse(job.duration)


@tagged("post_install", "-at_install")
class TestAutovacuum(TransactionCase):
    """Verify _gc_old_jobs deletes old done/failed jobs."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_gc_deletes_old_completed_jobs(self):
        """Jobs with completed_at older than retention period are deleted."""
        old_date = fields.Datetime.now() - timedelta(days=60)
        old_job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="gc_old",
        )
        old_job.write(
            {
                "state": "done",
                "completed_at": old_date,
            }
        )

        self.Job._gc_old_jobs()

        self.assertFalse(old_job.exists())

    def test_gc_preserves_recent_completed_jobs(self):
        """Jobs completed within retention period are kept."""
        recent_date = fields.Datetime.now() - timedelta(days=5)
        recent_job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="gc_recent",
        )
        recent_job.write(
            {
                "state": "done",
                "completed_at": recent_date,
            }
        )

        self.Job._gc_old_jobs()

        self.assertTrue(recent_job.exists())

    def test_gc_preserves_pending_jobs(self):
        """Pending jobs without completed_at are never deleted."""
        pending_job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="gc_pending",
        )

        self.Job._gc_old_jobs()

        self.assertTrue(pending_job.exists())

    def test_gc_deletes_old_failed_jobs(self):
        """Failed jobs with old completed_at are also cleaned up."""
        old_date = fields.Datetime.now() - timedelta(days=60)
        failed_job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="gc_failed",
        )
        failed_job.write(
            {
                "state": "failed",
                "completed_at": old_date,
                "exc_info": "Old failure",
            }
        )

        self.Job._gc_old_jobs()

        self.assertFalse(failed_job.exists())

    def test_gc_respects_custom_retention_days(self):
        """Custom retention via ir.config_parameter is honored."""
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "10"
        )
        # Job 15 days old — should be deleted with 10-day retention
        old_job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="gc_custom",
        )
        old_job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now() - timedelta(days=15),
            }
        )

        self.Job._gc_old_jobs()

        self.assertFalse(old_job.exists())


@tagged("post_install", "-at_install")
class TestGarbageCollectionEdgeCases(TransactionCase):
    """Probe garbage collection / autovacuum edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_gc_does_not_delete_pending_jobs(self):
        """GC should never delete pending jobs regardless of age."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "old pending"}],
            kwargs={},
            channel="gc",
        )
        # Force an old create_date via SQL
        self.env.cr.execute(
            "UPDATE queue_job SET create_date = NOW()"
            " - INTERVAL '365 days' WHERE id = %s",
            (job.id,),
        )
        self.env.cr.flush()
        self.Job._gc_old_jobs()
        self.assertTrue(self.Job.browse(job.id).exists())

    def test_gc_does_not_delete_started_jobs(self):
        """GC should never delete started (running) jobs."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "old started"}],
            kwargs={},
            channel="gc_started",
        )
        job.write({"state": "started"})
        self.env.cr.execute(
            "UPDATE queue_job SET create_date = NOW()"
            " - INTERVAL '365 days' WHERE id = %s",
            (job.id,),
        )
        self.env.cr.flush()
        self.Job._gc_old_jobs()
        self.assertTrue(self.Job.browse(job.id).exists())

    def test_gc_does_not_delete_waiting_jobs(self):
        """GC should never delete waiting jobs."""
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "gc parent"}],
            kwargs={},
            channel="gc_waiting",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "gc child"}],
            kwargs={},
            parent_id=parent.id,
            channel="gc_waiting",
        )
        self.assertEqual(child.state, "waiting")
        self.env.cr.execute(
            "UPDATE queue_job SET create_date = NOW()"
            " - INTERVAL '365 days' WHERE id = %s",
            (child.id,),
        )
        self.env.cr.flush()
        self.Job._gc_old_jobs()
        self.assertTrue(self.Job.browse(child.id).exists())

    def test_gc_deletes_old_done_jobs(self):
        """GC should delete done jobs older than retention."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "old done"}],
            kwargs={},
            channel="gc_done",
        )
        old_time = fields.Datetime.now() - timedelta(days=60)
        job.write({"state": "done", "completed_at": old_time})
        self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_deletes_old_failed_jobs(self):
        """GC should delete failed jobs older than retention."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "old failed"}],
            kwargs={},
            channel="gc_failed",
        )
        old_time = fields.Datetime.now() - timedelta(days=60)
        job.write({"state": "failed", "completed_at": old_time})
        self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_deletes_old_cancelled_jobs(self):
        """GC should delete cancelled jobs older than retention."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "old cancelled"}],
            kwargs={},
            channel="gc_cancelled",
        )
        old_time = fields.Datetime.now() - timedelta(days=60)
        job.write({"state": "cancelled", "cancelled_at": old_time})
        self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_respects_retention_parameter(self):
        """GC should respect the configured retention days."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "retention test"}],
            kwargs={},
            channel="gc_retention",
        )
        # Set completed 10 days ago
        old_time = fields.Datetime.now() - timedelta(days=10)
        job.write({"state": "done", "completed_at": old_time})

        # Set retention to 365 days - job should survive
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "365"
        )
        self.Job._gc_old_jobs()
        self.assertTrue(self.Job.browse(job.id).exists())

        # Set retention to 5 days - job should be deleted
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "5"
        )
        self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_zero_retention_deletes_all_terminal_jobs(self):
        """GC with 0 retention days should delete all completed jobs,
        including those completed at exactly now.
        Freeze time to ensure completed_at == cutoff, validating the <= fix."""
        fixed = datetime(2026, 6, 15, 12, 0, 0)
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "zero retention"}],
            kwargs={},
            channel="gc_zero",
        )
        job.write({"state": "done", "completed_at": fixed})
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "0"
        )
        with patch("odoo.fields.Datetime.now", return_value=fixed):
            self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_negative_retention_deletes_recent_jobs(self):
        """Negative retention days means cutoff is in the future,
        which will delete even recently completed jobs."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "recent job"}],
            kwargs={},
            channel="gc_neg_retention",
        )
        job.write({"state": "done", "completed_at": fields.Datetime.now()})
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "-1"
        )
        self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_zero_retention_deletes_cancelled_jobs_at_boundary(self):
        """GC with 0 retention days should delete cancelled jobs whose
        cancelled_at equals the cutoff exactly (the <= boundary)."""
        fixed = datetime(2026, 6, 15, 12, 0, 0)
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "zero retention cancelled"}],
            kwargs={},
            channel="gc_zero_cancelled",
        )
        job.write({"state": "cancelled", "cancelled_at": fixed})
        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "0"
        )
        with patch("odoo.fields.Datetime.now", return_value=fixed):
            self.Job._gc_old_jobs()
        self.assertFalse(self.Job.browse(job.id).exists())

    def test_gc_done_job_without_completed_at_survives(self):
        """A done job with NULL completed_at should not be deleted
        by GC since the SQL comparison with NULL is always false."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "null completed"}],
            kwargs={},
            channel="gc_null_completed",
        )
        # Force state to done without setting completed_at
        self.env.cr.execute(
            "UPDATE queue_job SET state = 'done', completed_at = NULL WHERE id = %s",
            (job.id,),
        )
        self.env.cr.flush()
        job.invalidate_recordset()

        self.env["ir.config_parameter"].sudo().set_param(
            "job_worker.done_job_retention_days", "0"
        )
        self.Job._gc_old_jobs()
        # NULL completed_at means the SQL <= comparison never matches
        self.assertTrue(self.Job.browse(job.id).exists())
