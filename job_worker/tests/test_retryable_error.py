from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

from odoo import SUPERUSER_ID, api, fields
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker
from ..exception import RetryableJobError


@tagged("post_install", "-at_install")
class TestRetryableJobErrorInRunNow(TransactionCase):
    """Test RetryableJobError handling in synchronous run_now() execution."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _patch_method(self, model_cls, method_name, replacement):
        """Patch a method and return the original for cleanup."""
        original = getattr(model_cls, method_name)
        setattr(model_cls, method_name, replacement)
        self.addCleanup(setattr, model_cls, method_name, original)

    def test_retryable_error_with_seconds_sets_scheduled_at(self):
        """RetryableJobError(seconds=N) uses fixed delay instead of backoff."""
        partner = self.env["res.partner"].create({"name": "Retry Partner"})
        job = partner.with_delay(max_retries=3).write({"name": "Nope"})

        def raising_write(self_rec, vals):
            raise RetryableJobError("transient", seconds=42)

        self._patch_method(type(partner), "write", raising_write)
        job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 1)
        self.assertTrue(job.scheduled_at)

    def test_retryable_error_with_ignore_retry_does_not_increment_attempts(self):
        """RetryableJobError(ignore_retry=True) does not count the attempt."""
        partner = self.env["res.partner"].create({"name": "Ignore Retry"})
        job = partner.with_delay(max_retries=3).write({"name": "Nope"})

        def raising_write(self_rec, vals):
            raise RetryableJobError("soft", ignore_retry=True)

        self._patch_method(type(partner), "write", raising_write)
        job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)

    def test_retryable_error_without_seconds_uses_backoff(self):
        """RetryableJobError(seconds=None) falls back to exponential backoff."""
        partner = self.env["res.partner"].create({"name": "Backoff Partner"})
        job = partner.with_delay(max_retries=3).write({"name": "Nope"})

        def raising_write(self_rec, vals):
            raise RetryableJobError("no seconds")

        self._patch_method(type(partner), "write", raising_write)
        job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 1)
        self.assertTrue(job.scheduled_at)

    def test_retryable_error_with_both_seconds_and_ignore(self):
        """RetryableJobError with both seconds and ignore_retry."""
        partner = self.env["res.partner"].create({"name": "Both Params"})
        job = partner.with_delay(max_retries=1).write({"name": "Nope"})

        def raising_write(self_rec, vals):
            raise RetryableJobError("both", seconds=120, ignore_retry=True)

        self._patch_method(type(partner), "write", raising_write)
        job.run_now()

        self.assertEqual(job.attempts, 0)
        self.assertEqual(job.state, "pending")
        self.assertTrue(job.scheduled_at)

    def test_retryable_error_does_not_propagate(self):
        """RetryableJobError is handled silently -- not re-raised."""
        partner = self.env["res.partner"].create({"name": "No Propagate"})
        job = partner.with_delay().write({"name": "Nope"})

        def raising_write(self_rec, vals):
            raise RetryableJobError("silent")

        self._patch_method(type(partner), "write", raising_write)
        # Should NOT raise
        job.run_now()
        self.assertEqual(job.state, "pending")


@tagged("post_install", "-at_install")
class TestRetryLogicEdgeCases(TransactionCase):
    """Probe retry/backoff logic for overflow, edge cases, and abuse."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def test_negative_max_retries_treated_as_no_retries(self):
        """A negative max_retries should fail immediately on first error
        when processed through the worker's handle_exception path."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=-1,
                channel="retry_neg_retry",
            )
            worker.handle_exception(job, exc=Exception("fail"))
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "failed")
            self.assertEqual(refreshed.attempts, 1)

    def test_very_large_max_retries_does_not_overflow(self):
        """Extremely large max_retries should not cause overflow
        in the worker's retry logic."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=999999,
                channel="retry_huge_retry",
            )
            worker.handle_exception(job, exc=Exception("fail"))
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            # Should retry (not fail permanently)
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 1)

    def test_backoff_with_huge_attempt_count_does_not_overflow(self):
        """Exponential backoff with huge attempts should be capped, not overflow."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname, max_backoff_seconds=3600)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="method_does_not_exist",
                record_ids=[],
                args=[],
                kwargs={},
                max_retries=0,
                channel="retry_overflow",
            )
            # Simulate an absurdly high attempt count
            job.write({"attempts": 1000})

            fixed = datetime(2026, 1, 1, 0, 0, 0)
            with patch("odoo.fields.Datetime.now", return_value=fixed):
                worker.handle_exception(job)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            scheduled = fields.Datetime.to_datetime(refreshed.scheduled_at)
            # Should be capped at max_backoff_seconds
            self.assertEqual(scheduled, fixed + timedelta(seconds=3600))

    def test_retryable_error_with_zero_seconds_delay(self):
        """RetryableJobError with seconds=0 should schedule for immediate retry."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "retry0"}],
                kwargs={},
                max_retries=3,
                channel="retry_zero_sec",
            )
            exc = RetryableJobError("zero delay", seconds=0)
            worker.handle_exception(job, exc=exc)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            # scheduled_at should be approximately now (0 second delay)
            diff = abs(
                (
                    fields.Datetime.to_datetime(refreshed.scheduled_at)
                    - fields.Datetime.now()
                ).total_seconds()
            )
            self.assertLess(diff, 5)

    def test_retryable_error_with_negative_seconds_delay(self):
        """RetryableJobError with negative seconds schedules in the past."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "neg delay"}],
                kwargs={},
                max_retries=3,
                channel="retry_neg_delay",
            )
            before = fields.Datetime.now()
            exc = RetryableJobError("negative delay", seconds=-60)
            worker.handle_exception(job, exc=exc)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            # scheduled_at should be in the past
            self.assertTrue(
                fields.Datetime.to_datetime(refreshed.scheduled_at) < before
            )

    def test_retryable_error_ignore_retry_does_not_count_attempt(self):
        """RetryableJobError with ignore_retry=True should not increment attempts."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "ignore"}],
                kwargs={},
                max_retries=1,
                channel="retry_ignore_retry",
            )
            exc = RetryableJobError("transient", ignore_retry=True)
            worker.handle_exception(job, exc=exc)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            self.assertEqual(refreshed.attempts, 0)

    def test_retryable_error_with_very_large_seconds(self):
        """RetryableJobError with an extremely large delay should not crash."""
        with self._external_env() as (cr, env):
            worker = QueueWorker(cr.dbname)
            job = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="write",
                record_ids=[],
                args=[{"name": "huge delay"}],
                kwargs={},
                max_retries=3,
                channel="retry_huge_delay",
            )
            exc = RetryableJobError("huge delay", seconds=999999999)
            worker.handle_exception(job, exc=exc)
            env.invalidate_all()
            refreshed = env["queue.job"].browse(job.id)
            self.assertEqual(refreshed.state, "pending")
            self.assertTrue(refreshed.scheduled_at)


@tagged("post_install", "-at_install")
class TestRunNowRetryableEdgeCases(TransactionCase):
    """Probe run_now() handling of RetryableJobError edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_run_now_retryable_error_schedules_retry(self):
        """run_now() with RetryableJobError should schedule a retry."""
        partner = self.env["res.partner"].create({"name": "Retry Test"})

        def _raise_retryable(records, vals):
            raise RetryableJobError("please retry", seconds=120)

        with patch.object(
            type(self.env["res.partner"]),
            "queue_raise_retryable",
            _raise_retryable,
            create=True,
        ):
            job = self.Job.enqueue(
                model_name="res.partner",
                method_name="queue_raise_retryable",
                record_ids=partner.ids,
                args=[{"name": "After Retry"}],
                kwargs={},
                max_retries=3,
                channel="run_now_retry",
            )
            job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 1)
        self.assertTrue(job.scheduled_at)

    def test_run_now_retryable_error_with_ignore_retry(self):
        """run_now() with RetryableJobError(ignore_retry=True) should not
        increment attempts."""
        partner = self.env["res.partner"].create({"name": "Ignore Retry Test"})

        def _raise_retryable_ignore(records, vals):
            raise RetryableJobError("transient", ignore_retry=True)

        with patch.object(
            type(self.env["res.partner"]),
            "queue_raise_retryable_ignore",
            _raise_retryable_ignore,
            create=True,
        ):
            job = self.Job.enqueue(
                model_name="res.partner",
                method_name="queue_raise_retryable_ignore",
                record_ids=partner.ids,
                args=[{"name": "After"}],
                kwargs={},
                max_retries=1,
                channel="run_now_ignore",
            )
            job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)

    def test_run_now_retryable_exponential_backoff(self):
        """run_now() with RetryableJobError (no explicit seconds) should
        use exponential backoff."""
        partner = self.env["res.partner"].create({"name": "Backoff Test"})

        def _raise_retryable_no_seconds(records, vals):
            raise RetryableJobError("retry me")

        with patch.object(
            type(self.env["res.partner"]),
            "queue_raise_retryable_exp",
            _raise_retryable_no_seconds,
            create=True,
        ):
            job = self.Job.enqueue(
                model_name="res.partner",
                method_name="queue_raise_retryable_exp",
                record_ids=partner.ids,
                args=[{"name": "After"}],
                kwargs={},
                max_retries=5,
                channel="run_now_backoff",
            )
            before = fields.Datetime.now()
            job.run_now()

        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 1)
        scheduled = fields.Datetime.to_datetime(job.scheduled_at)
        # First attempt: 10 * 2^0 = 10 seconds
        expected_min = before + timedelta(seconds=8)
        self.assertGreaterEqual(scheduled, expected_min)
