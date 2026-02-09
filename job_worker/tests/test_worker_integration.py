import uuid
from contextlib import contextmanager

import odoo
from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerIntegration(TransactionCase):
    def setUp(self):
        super().setUp()
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            cr.commit()

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def test_process_jobs_drains_pending_queue(self):
        token = uuid.uuid4().hex
        channel = f"batch_{token}"
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            jobs = [
                env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="create",
                    record_ids=[],
                    args=[{"name": f"Batch Partner {token}-{index}"}],
                    kwargs={},
                    channel=channel,
                )
                for index in range(3)
            ]
            job_ids = [job.id for job in jobs]
            cr.commit()

        worker = QueueWorker(self.env.cr.dbname)
        processed = 0
        max_iterations = len(job_ids) + 3
        db = odoo.sql_db.db_connect(self.env.cr.dbname)
        for _ in range(max_iterations):
            with db.cursor() as cr:
                job_id = worker.acquire_job_lock(cr)
                if not job_id:
                    break
                cr.commit()
                worker.execute_job(cr, job_id)
                processed += 1
        self.assertEqual(processed, len(job_ids))

        with self._external_env() as (_, env):
            job_states = env["queue.job"].browse(job_ids).mapped("state")
            self.assertEqual(set(job_states), {"done"})

    def test_two_workers_do_not_run_same_channel_in_parallel_when_limit_is_one(self):
        token = uuid.uuid4().hex
        channel = f"serialized_{token}"
        with self._external_env() as (cr, env):
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            env["queue.limit"].create({"name": channel, "limit": 1})
            first = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel=channel,
            )
            second = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel=channel,
            )
            job_ids = [first.id, second.id]
            cr.commit()

        worker_a = QueueWorker(self.env.cr.dbname)
        worker_b = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        with db.cursor() as cr_a:
            acquired_a = worker_a.acquire_job_lock(cr_a)
            self.assertIn(acquired_a, set(job_ids))
            cr_a.commit()

        with db.cursor() as cr_b:
            acquired_b_first_try = worker_b.acquire_job_lock(cr_b)
            self.assertFalse(acquired_b_first_try)

        with self._external_env() as (cr, env):
            env["queue.job"].browse(acquired_a).write({"state": "done"})
            cr.commit()

        with db.cursor() as cr_b_2:
            acquired_b_second_try = worker_b.acquire_job_lock(cr_b_2)
            self.assertIn(acquired_b_second_try, set(job_ids))
            self.assertNotEqual(acquired_a, acquired_b_second_try)

        with self._external_env() as (_, env):
            states = env["queue.job"].browse(job_ids).mapped("state")
            self.assertIn("started", states)
            self.assertIn("done", states)
