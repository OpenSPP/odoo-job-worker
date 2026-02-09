from contextlib import closing
from datetime import timedelta

from psycopg2 import IntegrityError

import odoo
from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerAcquisition(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.Limit = self.env["queue.limit"]
        self.worker = QueueWorker(self.env.cr.dbname)
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            cr.commit()

    def _enqueue_search_job(self, **kwargs):
        return self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            **kwargs,
        )

    def test_acquires_highest_priority_first(self):
        low = self._enqueue_search_job(priority=30, channel="prio")
        high = self._enqueue_search_job(priority=1, channel="prio")
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, high.id)
        low.invalidate_recordset()
        high.invalidate_recordset()
        self.assertEqual(low.state, "pending")
        self.assertEqual(high.state, "started")

    def test_does_not_acquire_future_scheduled_job(self):
        future = fields.Datetime.now() + timedelta(minutes=10)
        job = self._enqueue_search_job(priority=5, channel="eta", scheduled_at=future)
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertFalse(acquired_job_id)
        job.invalidate_recordset()
        self.assertEqual(job.state, "pending")

    def test_acquires_job_when_schedule_is_due(self):
        due = fields.Datetime.now() - timedelta(seconds=1)
        job = self._enqueue_search_job(priority=5, channel="eta_due", scheduled_at=due)
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, job.id)
        job.invalidate_recordset()
        self.assertEqual(job.state, "started")
        self.assertEqual(job.worker_id, self.worker.worker_uuid)

    def test_default_channel_limit_is_one(self):
        running = self._enqueue_search_job(channel="default_limit")
        pending = self._enqueue_search_job(channel="default_limit")
        running.write(
            {
                "state": "started",
                "heartbeat": fields.Datetime.now(),
                "worker_id": "other",
            }
        )
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertFalse(acquired_job_id)
        pending.invalidate_recordset()
        self.assertEqual(pending.state, "pending")

    def test_named_limit_allows_when_capacity_available(self):
        self.Limit.create({"name": "throttled", "limit": 2})
        running = self._enqueue_search_job(channel="throttled")
        pending = self._enqueue_search_job(channel="throttled")
        running.write(
            {
                "state": "started",
                "heartbeat": fields.Datetime.now(),
                "worker_id": "other",
            }
        )
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, pending.id)
        pending.invalidate_recordset()
        self.assertEqual(pending.state, "started")

    def test_started_job_with_fresh_heartbeat_is_not_stolen(self):
        job = self._enqueue_search_job(channel="active")
        job.write(
            {
                "state": "started",
                "heartbeat": fields.Datetime.now(),
                "worker_id": "other",
            }
        )
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertFalse(acquired_job_id)

    def test_started_job_with_stale_heartbeat_is_reacquired(self):
        job = self._enqueue_search_job(channel="stale")
        job.write(
            {
                "state": "started",
                "heartbeat": fields.Datetime.now() - timedelta(seconds=120),
                "worker_id": "old-worker",
            }
        )
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, job.id)
        job.invalidate_recordset()
        self.assertEqual(job.state, "started")
        self.assertEqual(job.worker_id, self.worker.worker_uuid)
        self.assertTrue(job.heartbeat > fields.Datetime.now() - timedelta(seconds=5))

    def test_rate_limit_blocks_second_job_in_same_second(self):
        self.Limit.create({"name": "rps", "limit": 10, "rate_limit": 1})
        first = self._enqueue_search_job(channel="rps")
        self.env.flush_all()
        self.assertEqual(self.worker.acquire_job_lock(self.env.cr), first.id)

        second = self._enqueue_search_job(channel="rps")
        self.env.flush_all()
        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertFalse(acquired_job_id)
        second.invalidate_recordset()
        self.assertEqual(second.state, "pending")

    def test_equal_priority_jobs_are_acquired_by_smallest_id(self):
        first = self._enqueue_search_job(priority=7, channel="tie_break")
        second = self._enqueue_search_job(priority=7, channel="tie_break")
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, first.id)
        second.invalidate_recordset()
        self.assertEqual(second.state, "pending")

    def test_busy_channel_does_not_block_other_channels(self):
        busy_running = self._enqueue_search_job(channel="busy")
        busy_pending = self._enqueue_search_job(channel="busy")
        other_pending = self._enqueue_search_job(channel="other")
        busy_running.write(
            {
                "state": "started",
                "heartbeat": fields.Datetime.now(),
                "worker_id": "other",
            }
        )
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired_job_id, other_pending.id)
        busy_pending.invalidate_recordset()
        self.assertEqual(busy_pending.state, "pending")

    def test_limit_zero_blocks_channel(self):
        self.Limit.create({"name": "blocked", "limit": 0, "rate_limit": 0})
        job = self._enqueue_search_job(channel="blocked")
        self.env.flush_all()

        acquired_job_id = self.worker.acquire_job_lock(self.env.cr)

        self.assertFalse(acquired_job_id)
        job.invalidate_recordset()
        self.assertEqual(job.state, "pending")

    def test_limit_negative_values_are_rejected(self):
        with self.assertRaises(IntegrityError):
            self.Limit.create({"name": "bad_limit", "limit": -1})
            self.env.flush_all()
        with self.assertRaises(IntegrityError):
            self.Limit.create({"name": "bad_rate", "limit": 1, "rate_limit": -1})
            self.env.flush_all()

    def test_skip_locked_acquires_different_rows_with_two_cursors(self):
        with self.env.registry.cursor() as setup_cr:
            setup_env = api.Environment(setup_cr, SUPERUSER_ID, {})
            setup_env["queue.job"].search(
                [("state", "in", ["pending", "started"])]
            ).write({"state": "done", "heartbeat": False, "worker_id": False})
            setup_env["queue.limit"].create({"name": "skip_locked", "limit": 10})
            first_job = setup_env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", setup_env.user.id)]],
                kwargs={},
                channel="skip_locked",
            )
            second_job = setup_env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", setup_env.user.id)]],
                kwargs={},
                channel="skip_locked",
            )
            first_id = first_job.id
            second_id = second_job.id
            setup_cr.commit()

        worker_a = QueueWorker(self.env.cr.dbname)
        worker_b = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        with closing(db.cursor()) as cr_a, closing(db.cursor()) as cr_b:
            acquired_a = worker_a.acquire_job_lock(cr_a)
            acquired_b = worker_b.acquire_job_lock(cr_b)

        self.assertIn(acquired_a, {first_id, second_id})
        self.assertIn(acquired_b, {first_id, second_id})
        self.assertNotEqual(acquired_a, acquired_b)
