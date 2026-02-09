from odoo.tests.common import TransactionCase, tagged

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
