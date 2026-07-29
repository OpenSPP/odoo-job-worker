import threading
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
            # NOT released to 'pending' here: a timed-out job stays 'started'
            # so it cannot be re-dispatched while its abandoned execution
            # thread may still be running (see _handle_timeout). The reason is
            # stamped so the hold is diagnosable.
            self.assertEqual(refreshed.state, "started")
            self.assertIn("timeout", (refreshed.exc_info or "").lower())
            # Attempt accounting belongs to the reclaim path, which is now the
            # single writer for an attempt that never completed.
            self.assertEqual(refreshed.attempts, 0)
            # This thread HAS returned, so it released its own claim — which is
            # what makes the job eligible again.
            self.assertFalse(refreshed.worker_id)
            self.assertFalse(refreshed.heartbeat)

            # ...and it is genuinely re-acquirable, counting the attempt.
            reclaimer = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            self.assertEqual(reclaimer.acquire_job_lock(cr), job.id)
            env.invalidate_all()
            reclaimed = env["queue.job"].browse(job.id)
            self.assertEqual(reclaimed.state, "started")
            self.assertEqual(reclaimed.attempts, 1)

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
                # Pre-set attempts so the next attempt exhausts retries
                job.write(
                    {
                        "state": "started",
                        "worker_id": worker.worker_uuid,
                        "started_at": fields.Datetime.now(),
                        "attempts": 1,
                    }
                )
                worker.execute_job(cr, job.id)

            # The permanent-failure decision now belongs to the reclaim path
            # (the single writer for a non-completing attempt), so drive it.
            reclaimer = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            self.assertIsNone(
                reclaimer.acquire_job_lock(cr),
                "A job whose retries are exhausted must not be handed out again.",
            )

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "failed")
            self.assertTrue(refreshed.completed_at)
            exc_info = refreshed.exc_info or ""
            # Both diagnoses survive: the specific cause (it overran its
            # timeout) and the disposition (retries exhausted). The reclaim path
            # appends rather than overwriting precisely so the first is not lost.
            self.assertIn("timeout", exc_info.lower())
            self.assertIn("exhausted max_retries", exc_info)

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

    def test_timed_out_job_is_not_reacquirable_while_its_thread_still_runs(self):
        """The property the timeout hold exists for.

        A blocked Python thread cannot be killed, so when the timeout fires the
        job's method is still executing. If the row were released then, another
        worker would start the same method within one backoff and the two would
        run concurrently. The "results are discarded" rollback at the end of
        ``execute_job`` does not save us: it only covers the abandoned thread's
        own cursor, and a long job that commits internally — the kind that
        overruns a timeout — has already persisted its work.

        So while the execution thread is alive the job must be invisible to
        ``acquire_job_lock``, and only become eligible once it has returned.
        """
        with self._external_env() as (cr, env):
            self._cleanup_pending_jobs(env)
            partner = env["res.partner"].create({"name": "Timeout Concurrency"})

            def very_slow_method(records, vals):
                time.sleep(6)
                return True

            with patch.object(
                type(env["res.partner"]),
                "queue_very_slow_method",
                very_slow_method,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_very_slow_method",
                    record_ids=partner.ids,
                    args=[{}],
                    kwargs={},
                    timeout=1,
                    max_retries=3,
                    channel="timeout_concurrency",
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

                executor = threading.Thread(
                    target=self._execute_on_own_cursor, args=(worker, job.id)
                )
                executor.start()
                try:
                    # The 1s timeout has fired by now; the method sleeps on.
                    time.sleep(3)
                    with self.env.registry.cursor() as probe_cr:
                        rival = QueueWorker(
                            probe_cr.dbname, heartbeat_interval_seconds=1
                        )
                        self.assertIsNone(
                            rival.acquire_job_lock(probe_cr),
                            "A timed-out job was handed to a second worker while "
                            "its first execution thread was still running it.",
                        )
                finally:
                    executor.join(timeout=30)
                self.assertFalse(executor.is_alive())

            # Once the abandoned thread has returned it releases its claim, and
            # only then does the job become eligible again.
            with self.env.registry.cursor() as after_cr:
                reclaimer = QueueWorker(after_cr.dbname, heartbeat_interval_seconds=1)
                self.assertEqual(reclaimer.acquire_job_lock(after_cr), job.id)

    def _execute_on_own_cursor(self, worker, job_id):
        """Run a job on its own cursor, as a real worker thread would."""
        with self.env.registry.cursor() as cr:
            worker.execute_job(cr, job_id)


@tagged("post_install", "-at_install")
class TestClearWorkerOwnershipIsScoped(TransactionCase):
    """``_clear_worker_ownership`` must only release the caller's own claim.

    An abandoned post-timeout thread can return long after its heartbeat went
    stale and the job was reclaimed by someone else. Clearing unconditionally
    would null the *new* owner's worker_id/heartbeat, and because the acquire
    query treats a 'started' row with a NULL heartbeat as immediately
    reclaimable, a third worker would start the job while the second was still
    running it — the very double-execution the timeout hold prevents.
    """

    def test_does_not_release_a_job_owned_by_another_worker(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            partner = env["res.partner"].create({"name": "Ownership Scope"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Ownership Scope After"}],
                kwargs={},
                channel="ownership_scope",
            )
            current_owner = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            heartbeat = fields.Datetime.now()
            job.write(
                {
                    "state": "started",
                    "worker_id": current_owner.worker_uuid,
                    "heartbeat": heartbeat,
                }
            )
            cr.commit()

            stale_thread = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            stale_thread._clear_worker_ownership(job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.worker_id, current_owner.worker_uuid)
            self.assertTrue(refreshed.heartbeat)

    def test_releases_its_own_claim(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            partner = env["res.partner"].create({"name": "Ownership Own"})
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=partner.ids,
                args=[{"name": "Ownership Own After"}],
                kwargs={},
                channel="ownership_own",
            )
            owner = QueueWorker(cr.dbname, heartbeat_interval_seconds=1)
            job.write(
                {
                    "state": "started",
                    "worker_id": owner.worker_uuid,
                    "heartbeat": fields.Datetime.now(),
                }
            )
            cr.commit()

            owner._clear_worker_ownership(job.id)

            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertFalse(refreshed.worker_id)
            self.assertFalse(refreshed.heartbeat)
