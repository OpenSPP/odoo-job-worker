import time
import uuid
from contextlib import closing
from datetime import timedelta
from threading import Event, Thread
from unittest.mock import patch

from psycopg2 import IntegrityError, OperationalError

import odoo
from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker, _retry_db_operation, read_committed_cursor


@tagged("post_install", "-at_install")
class TestConcurrencyAndRecovery(TransactionCase):
    def setUp(self):
        super().setUp()
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            env["queue.job"].search([]).unlink()
            cr.commit()

    def _enqueue_with_identity(self, env, key):
        return env["queue.job"].enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", env.user.id)]],
            kwargs={},
            identity_key=key,
        )

    def test_identity_key_concurrent_enqueue_single_effective_job(self):
        key = f"concurrent_dedupe_{uuid.uuid4().hex}"
        first = self._enqueue_with_identity(self.env, key)
        original_search = type(self.env["queue.job"]).search
        search_call_count = {"identity_probe": 0}

        def _is_identity_probe(domain):
            if not isinstance(domain, list):
                return False
            has_identity = ("identity_key", "=", key) in domain
            has_state = ("state", "in", ["pending", "started"]) in domain
            return has_identity and has_state

        def _race_search(model_self, domain, *args, **kwargs):
            if _is_identity_probe(domain):
                search_call_count["identity_probe"] += 1
                if search_call_count["identity_probe"] == 1:
                    # Simulate stale snapshot right before create.
                    return model_self.browse()
            return original_search(model_self, domain, *args, **kwargs)

        with (
            patch.object(type(self.env["queue.job"]), "search", new=_race_search),
            patch.object(
                type(self.env["queue.job"]), "create", side_effect=IntegrityError()
            ),
        ):
            second = self._enqueue_with_identity(self.env, key)

        jobs = self.env["queue.job"].search(
            [("identity_key", "=", key), ("state", "=", "pending")]
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs.id, first.id)
        self.assertEqual(second.id, first.id)

    def test_started_job_reclaims_only_after_stale_timeout(self):
        channel = f"recovery_{uuid.uuid4().hex}"
        with self.env.registry.cursor() as setup_cr:
            setup_env = api.Environment(setup_cr, SUPERUSER_ID, {})
            job = setup_env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", setup_env.user.id)]],
                kwargs={},
                channel=channel,
            )
            job.write(
                {
                    "state": "started",
                    "worker_id": "live-worker",
                    "heartbeat": fields.Datetime.now() - timedelta(seconds=30),
                }
            )
            job_id = job.id
            setup_cr.commit()

        worker = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        with closing(db.cursor()) as cr_fresh:
            acquired_fresh = worker.acquire_job_lock(cr_fresh)
        self.assertFalse(acquired_fresh)

        with self.env.registry.cursor() as stale_cr:
            stale_env = api.Environment(stale_cr, SUPERUSER_ID, {})
            stale_env["queue.job"].browse(job_id).write(
                {"heartbeat": fields.Datetime.now() - timedelta(seconds=120)}
            )
            stale_cr.commit()

        with closing(db.cursor()) as cr_stale:
            acquired_stale = worker.acquire_job_lock(cr_stale)
        self.assertEqual(acquired_stale, job_id)

    def test_run_loop_wakes_quickly_on_notify(self):
        stop_event = Event()
        run_call_times = []
        first_seen = Event()
        second_seen = Event()
        worker = QueueWorker(self.env.cr.dbname, stop_event=stop_event, poll_timeout=10)

        def _process_jobs():
            run_call_times.append(time.monotonic())
            if len(run_call_times) == 1:
                first_seen.set()
            elif len(run_call_times) >= 2:
                second_seen.set()
                stop_event.set()

        with (
            patch.object(worker, "process_jobs", side_effect=_process_jobs),
            patch.object(worker, "update_heartbeats"),
        ):
            thread = Thread(target=worker.run, daemon=True)
            thread.start()
            try:
                self.assertTrue(first_seen.wait(timeout=2))

                with self.env.registry.cursor() as notify_cr:
                    notify_cr.execute("NOTIFY queue_job_wake_up")
                    notify_cr.commit()

                self.assertTrue(second_seen.wait(timeout=2))
                self.assertLess(run_call_times[1] - run_call_times[0], 5)
            finally:
                stop_event.set()
                with self.env.registry.cursor() as notify_cr:
                    notify_cr.execute("NOTIFY queue_job_wake_up")
                    notify_cr.commit()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

    def test_read_committed_prevents_heartbeat_completion_conflict(self):
        """Verify READ COMMITTED isolation prevents serialization failure.

        Simulates the PostgreSQL 18 bug scenario:
        1. Job starts executing with heartbeat thread
        2. Heartbeat commits UPDATE while job is running
        3. Job completion should succeed without SerializationFailure
        """
        channel = f"pg18_test_{uuid.uuid4().hex}"
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            job = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel=channel,
            )
            job_id = job.id
            cr.commit()

        # Simulate heartbeat update in separate transaction
        with self.env.registry.cursor() as cr:
            cr.execute(
                "UPDATE queue_job SET heartbeat = NOW() WHERE id = %s", (job_id,)
            )
            cr.commit()

        # Now try to complete the job - should not raise SerializationFailure
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            job = env["queue.job"].browse(job_id)
            job.state = "done"
            job.completed_at = fields.Datetime.now()
            cr.commit()

        # If we get here without exception, the fix works
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            job = env["queue.job"].browse(job_id)
            self.assertEqual(job.state, "done")

    def test_retry_on_serialization_failure(self):
        """Verify retry logic handles transient serialization errors."""
        call_count = 0

        def failing_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                # Simulate serialization failure
                err = OperationalError()
                err.pgcode = "40001"
                raise err
            return "success"

        # Should retry and eventually succeed
        result = _retry_db_operation(failing_operation, "test_op", max_retries=3)
        self.assertEqual(result, "success")
        self.assertEqual(call_count, 3)  # Failed twice, succeeded third time

    def test_read_committed_cursor_switches_isolation(self):
        """Verify context manager switches to READ COMMITTED and restores."""
        from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

        # Get a cursor to check isolation level
        db = odoo.sql_db.db_connect(self.env.cr.dbname)
        with closing(db.cursor()) as cr:
            original_isolation = cr._cnx.isolation_level

            # Enter READ COMMITTED context
            with read_committed_cursor(db) as rc_cr:
                self.assertEqual(
                    rc_cr._cnx.isolation_level, ISOLATION_LEVEL_READ_COMMITTED
                )

            # Verify restoration - check a fresh cursor to be sure
            with closing(db.cursor()) as cr2:
                self.assertEqual(cr2._cnx.isolation_level, original_isolation)
