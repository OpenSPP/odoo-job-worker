import threading
import time
from contextlib import closing
from datetime import timedelta

import odoo
from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestWorkerSafety(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.job"].search([("state", "in", ["pending", "started"])]).write(
                {"state": "done", "heartbeat": False, "worker_id": False}
            )
            cr.commit()

    def _enqueue(self, channel):
        return self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel=channel,
        )

    def test_started_job_with_null_heartbeat_is_reclaimed(self):
        worker = QueueWorker(self.env.cr.dbname)
        job = self._enqueue("null_heartbeat")
        job.write({"state": "started", "worker_id": "lost-worker", "heartbeat": False})
        self.env.flush_all()

        acquired = worker.acquire_job_lock(self.env.cr)

        self.assertEqual(acquired, job.id)
        job.invalidate_recordset()
        self.assertEqual(job.state, "started")
        self.assertEqual(job.worker_id, worker.worker_uuid)
        self.assertTrue(job.heartbeat > fields.Datetime.now() - timedelta(seconds=5))

    def test_reclaimed_stale_started_job_is_acquired_once_with_two_cursors(self):
        with self.env.registry.cursor() as setup_cr:
            setup_env = api.Environment(setup_cr, SUPERUSER_ID, {})
            setup_env["queue.job"].search(
                [("state", "in", ["pending", "started"])]
            ).write({"state": "done", "heartbeat": False, "worker_id": False})
            job = setup_env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", setup_env.user.id)]],
                kwargs={},
                channel="stale_once",
            )
            job.write(
                {
                    "state": "started",
                    "worker_id": "stale-owner",
                    "heartbeat": fields.Datetime.now() - timedelta(seconds=120),
                }
            )
            job_id = job.id
            setup_cr.commit()

        worker_a = QueueWorker(self.env.cr.dbname)
        worker_b = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        with closing(db.cursor()) as cr_a:
            acquired_a = worker_a.acquire_job_lock(cr_a)
            cr_a.commit()
        with closing(db.cursor()) as cr_b:
            acquired_b = worker_b.acquire_job_lock(cr_b)

        self.assertEqual(acquired_a, job_id)
        self.assertFalse(acquired_b)

    def test_heartbeat_loop_prevents_stale_reacquire(self):
        with self.env.registry.cursor() as setup_cr:
            setup_env = api.Environment(setup_cr, SUPERUSER_ID, {})
            setup_env["queue.job"].search(
                [("state", "in", ["pending", "started"])]
            ).write({"state": "done", "heartbeat": False, "worker_id": False})
            job = setup_env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", setup_env.user.id)]],
                kwargs={},
                channel="heartbeat_guard",
            )
            worker_a = QueueWorker(
                setup_cr.dbname,
                stale_after_seconds=1,
                heartbeat_interval_seconds=1,
            )
            # Seed the heartbeat deliberately STALE. If the loop never refreshed
            # it, worker_b below WOULD acquire the job — which is what keeps this
            # test honest. Seeding it fresh instead would let the seed alone
            # satisfy the assertion, and the loop could stop working unnoticed.
            stale_heartbeat = fields.Datetime.now() - timedelta(seconds=120)
            job.write(
                {
                    "state": "started",
                    "worker_id": worker_a.worker_uuid,
                    "heartbeat": stale_heartbeat,
                }
            )
            job_id = job.id
            setup_cr.commit()

        # 2s, not 1s: the margin has to exceed the gap between observing the
        # refresh below and issuing the acquire. At 1s this test was a coin flip
        # on loaded CI -- _heartbeat_loop does stop_event.wait(interval) BEFORE
        # each write, so heartbeats land at t=1s, 2s, ... and the old fixed
        # sleep(2.1) probe sat ~0.1s after a write. Any hiccup that delayed the
        # t=2s write past the probe made the heartbeat read 1.1s old, i.e.
        # legitimately stale, and the job was reclaimed.
        worker_b = QueueWorker(self.env.cr.dbname, stale_after_seconds=2)
        stop_event = threading.Event()
        heartbeat_thread = threading.Thread(
            target=worker_a._heartbeat_loop, args=(job_id, stop_event), daemon=True
        )
        heartbeat_thread.start()
        try:
            # Wait for the refresh to actually land rather than guessing how long
            # it takes. This is what removes the race.
            self.assertTrue(
                self._wait_for_heartbeat_after(job_id, stale_heartbeat),
                "the heartbeat loop never refreshed the heartbeat",
            )
            db = odoo.sql_db.db_connect(self.env.cr.dbname)
            with closing(db.cursor()) as cr_b:
                acquired_b = worker_b.acquire_job_lock(cr_b)
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=3)
        self.assertFalse(acquired_b)

    def _wait_for_heartbeat_after(self, job_id, previous, timeout=15.0):
        """Poll until ``job_id``'s committed heartbeat is newer than *previous*.

        Read on its own short-lived cursor each time: the heartbeat is written by
        another thread on another connection, so a long-lived cursor would hold a
        snapshot from before the write and never see it.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.env.registry.cursor() as poll_cr:
                poll_cr.execute(
                    "SELECT heartbeat FROM queue_job WHERE id = %s", (job_id,)
                )
                row = poll_cr.fetchone()
            if row and row[0] and row[0] > previous:
                return True
            time.sleep(0.05)
        return False
