import time
from contextlib import contextmanager
from unittest.mock import patch

from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker
from .common import trap_jobs


class TestTimeoutField(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.partner = cls.env["res.partner"].create({"name": "Timeout Test"})

    def test_timeout_field_stored_on_job(self):
        job = self.env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=self.partner.ids,
            args=[{"name": "updated"}],
            kwargs={},
            timeout=300,
        )
        self.assertEqual(job.timeout, 300)

    def test_timeout_defaults_to_zero(self):
        job = self.env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=self.partner.ids,
            args=[{"name": "updated"}],
            kwargs={},
        )
        self.assertEqual(job.timeout, 0)


class TestTimeoutThroughAPI(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.partner = cls.env["res.partner"].create({"name": "Timeout API Test"})

    def test_with_delay_passes_timeout(self):
        with trap_jobs(self.env) as trap:
            self.partner.with_delay(timeout=120).write({"name": "delayed"})
            trap.assert_jobs_count(1)
            job_spec = trap.enqueued_jobs[0]
            self.assertEqual(job_spec["timeout"], 120)
            self.assertEqual(job_spec["job_record"].timeout, 120)

    def test_delayable_passes_timeout(self):
        with trap_jobs(self.env) as trap:
            self.partner.delayable(timeout=180).write({"name": "d"}).delay()
            trap.assert_jobs_count(1)
            job_spec = trap.enqueued_jobs[0]
            self.assertEqual(job_spec["timeout"], 180)
            self.assertEqual(job_spec["job_record"].timeout, 180)

    def test_delayable_set_timeout(self):
        with trap_jobs(self.env) as trap:
            d = self.partner.delayable()
            d.set(timeout=240)
            d.write({"name": "s"}).delay()
            trap.assert_jobs_count(1)
            self.assertEqual(trap.enqueued_jobs[0]["job_record"].timeout, 240)

    def test_split_preserves_timeout(self):
        partners = self.env["res.partner"].create(
            [{"name": f"Split {i}"} for i in range(4)]
        )
        with trap_jobs(self.env) as trap:
            d = partners.delayable(timeout=360)
            d.write({"name": "split"})
            grp = d.split(2)
            grp.delay()
            self.assertTrue(len(trap.enqueued_jobs) >= 2)
            for job_spec in trap.enqueued_jobs:
                self.assertEqual(job_spec["job_record"].timeout, 360)

    def test_timeout_default_zero_through_with_delay(self):
        with trap_jobs(self.env) as trap:
            self.partner.with_delay().write({"name": "no timeout"})
            trap.assert_jobs_count(1)
            self.assertEqual(trap.enqueued_jobs[0]["job_record"].timeout, 0)


@tagged("post_install", "-at_install")
class TestTimeoutWorkerEnforcement(TransactionCase):
    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def _cleanup_pending_jobs(self, env):
        env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
            {"state": "done", "heartbeat": False, "worker_id": False}
        )

    def test_job_within_timeout_completes_normally(self):
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "Timeout Normal"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Timeout Normal After"}],
                kwargs={},
                timeout=300,
                channel="timeout_normal",
            )
            worker = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "done")
            self.assertEqual(refreshed.timeout, 300)

    def test_timeout_zero_means_no_timeout(self):
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "No Timeout"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "No Timeout After"}],
                kwargs={},
                timeout=0,
                channel="timeout_zero",
            )
            worker = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "done")

    def test_timed_out_job_triggers_retry(self):
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "Timeout Retry"})

            def slow_method(records, vals):
                time.sleep(3)
                return True

            with patch.object(
                type(env["res.partner"]),
                "queue_slow_method",
                slow_method,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_slow_method",
                    record_ids=partner.ids,
                    args=[{}],
                    kwargs={},
                    timeout=1,
                    max_retries=3,
                    channel="timeout_retry",
                )
                worker = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
                job.write(
                    {
                        "state": "started",
                        "worker_id": worker.worker_uuid,
                        "started_at": fields.Datetime.now(),
                    }
                )
                worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 1)
            self.assertIn("timeout", (refreshed.exc_info or "").lower())

    def test_timed_out_job_fails_permanently_when_retries_exhausted(self):
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "Timeout Perm Fail"})

            def slow_method(records, vals):
                time.sleep(3)
                return True

            with patch.object(
                type(env["res.partner"]),
                "queue_slow_method",
                slow_method,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_slow_method",
                    record_ids=partner.ids,
                    args=[{}],
                    kwargs={},
                    timeout=1,
                    max_retries=1,
                    channel="timeout_permfail",
                )
                worker = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
                # Pre-set attempts so next timeout exhausts retries
                job.write(
                    {
                        "state": "started",
                        "worker_id": worker.worker_uuid,
                        "started_at": fields.Datetime.now(),
                        "attempts": 1,
                    }
                )
                worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "failed")
            self.assertTrue(refreshed.completed_at)
            self.assertIn("timeout", (refreshed.exc_info or "").lower())

    def test_timed_out_job_rolls_back_side_effects(self):
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "Rollback Original"})
            cr.commit()

            def slow_write_with_side_effect(records, vals):
                records.write({"name": "Side Effect Written"})
                time.sleep(3)
                return True

            with patch.object(
                type(env["res.partner"]),
                "queue_slow_side_effect",
                slow_write_with_side_effect,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_slow_side_effect",
                    record_ids=partner.ids,
                    args=[{}],
                    kwargs={},
                    timeout=1,
                    max_retries=3,
                    channel="timeout_rollback",
                )
                worker = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
                job.write(
                    {
                        "state": "started",
                        "worker_id": worker.worker_uuid,
                        "started_at": fields.Datetime.now(),
                    }
                )
                cr.commit()
                worker.execute_job(cr, job.id)

            env.invalidate_all()
            refreshed_partner = env["res.partner"].browse(partner.id)
            self.assertEqual(refreshed_partner.name, "Rollback Original")
