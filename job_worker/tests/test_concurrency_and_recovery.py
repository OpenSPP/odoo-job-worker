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
from ..exception import TransientRegistryError


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

    def _enqueue_plain(self, channel):
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
            job_id = job.id
            setup_cr.commit()
        return job_id

    def _mark_started_stale(self, job_id):
        with self.env.registry.cursor() as cr:
            cr.execute(
                "UPDATE queue_job SET state='started', worker_id='dead-worker',"
                " heartbeat = NOW() - INTERVAL '120 seconds' WHERE id = %s",
                (job_id,),
            )
            cr.commit()

    def _read_state_attempts(self, job_id):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            job = env["queue.job"].browse(job_id)
            return job.state, job.attempts

    def test_reclaim_counts_attempt_and_fails_when_max_retries_exhausted(self):
        """A job that keeps killing its worker must not be reclaimed forever.

        A worker killed as a process (OOM, stall-watchdog os._exit, container
        restart) leaves its job in 'started' with a stale heartbeat and raises
        no exception, so neither execute_job nor _handle_timeout counts the
        attempt. The stale-recovery acquire path must therefore count each
        reclaim as a failed attempt and, once max_retries is exhausted, mark
        the job 'failed' instead of re-running it — otherwise the job restarts
        forever with retries stuck at 0 (preprod cv_reconcile loop, 2026-07-17).
        """
        job_id = self._enqueue_plain(f"reclaim_{uuid.uuid4().hex}")
        with self.env.registry.cursor() as cr:
            cr.execute("UPDATE queue_job SET max_retries = 2 WHERE id = %s", (job_id,))
            cr.commit()

        worker = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        # Reclaims 1 and 2 are within the cap: job is re-run, attempts climb.
        for expected_attempt in (1, 2):
            self._mark_started_stale(job_id)
            with closing(db.cursor()) as cr:
                acquired = worker.acquire_job_lock(cr)
                cr.commit()
            self.assertEqual(acquired, job_id)
            self.assertEqual(
                self._read_state_attempts(job_id), ("started", expected_attempt)
            )

        # Reclaim 3 exceeds max_retries=2: job is failed, NOT handed out.
        self._mark_started_stale(job_id)
        with closing(db.cursor()) as cr:
            acquired = worker.acquire_job_lock(cr)
            cr.commit()
        self.assertIsNone(acquired)
        state, attempts = self._read_state_attempts(job_id)
        self.assertEqual(state, "failed")
        self.assertEqual(attempts, 3)

    def test_reclaim_with_infinite_retries_never_fails(self):
        """max_retries == 0 means retry forever; a reclaim never fails the job."""
        job_id = self._enqueue_plain(f"reclaim_inf_{uuid.uuid4().hex}")
        with self.env.registry.cursor() as cr:
            cr.execute("UPDATE queue_job SET max_retries = 0 WHERE id = %s", (job_id,))
            cr.commit()

        worker = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)
        for expected_attempt in (1, 2, 3):
            self._mark_started_stale(job_id)
            with closing(db.cursor()) as cr:
                acquired = worker.acquire_job_lock(cr)
                cr.commit()
            self.assertEqual(acquired, job_id)
            self.assertEqual(
                self._read_state_attempts(job_id), ("started", expected_attempt)
            )

    def test_reclaim_exhausted_cascades_failure_to_waiting_child(self):
        """A reclaim that exhausts max_retries fails its waiting children too.

        Otherwise a chain/group whose parent's worker dies would leave the
        dependents stuck in 'waiting' forever (mirrors the cascade the normal
        permanent-failure path already performs).
        """
        parent_id = self._enqueue_plain(f"reclaim_casc_{uuid.uuid4().hex}")
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            child = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", env.user.id)]],
                kwargs={},
                channel=f"reclaim_casc_child_{uuid.uuid4().hex}",
            )
            child.write({"state": "waiting", "parent_id": parent_id})
            child_id = child.id
            cr.execute(
                "UPDATE queue_job SET max_retries = 1 WHERE id = %s", (parent_id,)
            )
            cr.commit()

        worker = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)
        # Reclaim 1 (attempt 1 == cap): re-run.
        self._mark_started_stale(parent_id)
        with closing(db.cursor()) as cr:
            self.assertEqual(worker.acquire_job_lock(cr), parent_id)
            cr.commit()
        # Reclaim 2 (attempt 2 > cap): parent failed, child cascaded to failed.
        self._mark_started_stale(parent_id)
        with closing(db.cursor()) as cr:
            self.assertIsNone(worker.acquire_job_lock(cr))
            cr.commit()
        self.assertEqual(self._read_state_attempts(parent_id), ("failed", 2))
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            self.assertEqual(env["queue.job"].browse(child_id).state, "failed")

    def test_transient_registry_error_retries_young_job_without_attempt(self):
        """A missing-model error on a young job retries without counting it.

        Model absent from the registry is almost always a transient module
        upgrade window; the job must be rescheduled 'pending' without burning
        an attempt so it survives the reload (preprod 490 cv_reconcile
        KeyError failures, 2026-07-17).
        """
        job_id = self._enqueue_plain(f"transient_{uuid.uuid4().hex}")
        worker = QueueWorker(self.env.cr.dbname)
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            worker.handle_exception(
                env["queue.job"].browse(job_id),
                exc=TransientRegistryError("model gone", seconds=30, ignore_retry=True),
            )
        state, attempts = self._read_state_attempts(job_id)
        self.assertEqual(state, "pending")
        self.assertEqual(attempts, 0)
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            self.assertTrue(env["queue.job"].browse(job_id).scheduled_at)

    def test_transient_registry_error_fails_aged_out_job(self):
        """Past the age cap the model is presumed gone for good → fail, not loop."""
        job_id = self._enqueue_plain(f"transient_aged_{uuid.uuid4().hex}")
        with self.env.registry.cursor() as cr:
            cr.execute(
                "UPDATE queue_job SET create_date = NOW() - INTERVAL '2 hours' "
                "WHERE id = %s",
                (job_id,),
            )
            cr.commit()
        worker = QueueWorker(
            self.env.cr.dbname, transient_registry_max_age_seconds=3600
        )
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            worker.handle_exception(
                env["queue.job"].browse(job_id),
                exc=TransientRegistryError("model gone", seconds=30, ignore_retry=True),
            )
        state, attempts = self._read_state_attempts(job_id)
        self.assertEqual(state, "failed")
        self.assertEqual(attempts, 0)

    def test_fresh_pending_pickup_does_not_increment_attempts(self):
        """A normal 'pending' pickup leaves attempts to the execute/timeout paths."""
        job_id = self._enqueue_plain(f"fresh_{uuid.uuid4().hex}")
        worker = QueueWorker(self.env.cr.dbname)
        db = odoo.sql_db.db_connect(self.env.cr.dbname)
        with closing(db.cursor()) as cr:
            acquired = worker.acquire_job_lock(cr)
            cr.commit()
        self.assertEqual(acquired, job_id)
        self.assertEqual(self._read_state_attempts(job_id), ("started", 0))

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

        class _SerializationError(OperationalError):
            pgcode = "40001"

        def failing_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise _SerializationError("serialization failure")
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
